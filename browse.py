#!/usr/bin/env python
# -*- coding: utf-8 -*-

from PyQt4 import QtGui, uic, QtCore
from PyQt4.QtCore import Qt

import os, os.path as path, sys, time, signal
from ConfigParser import ConfigParser
from optparse import OptionParser
import subprocess

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config
config = config.read()

PREVIEW_STYLESHEET = """
#preview {
  min-width: 400px;
  padding: 18px 18px 5px 18px;
}

#assetName {
  font-size: 18px;
}

"""

class Folder:
    def __init__(self, db, parent, name, row):
        self._db = db
        self.name = name
        self.parent = parent
        self.row = row
        if parent:
            self.path = parent.path + [name]
        else:
            self.path = name
        self.idx = None
        self._sort = None

    def _scan(self):
        print "Scanning", self.path
#        traceback.print_stack()
        self._sort = list(self._db.dir("path", self.path).iteritems())
        self._sort.sort(key=lambda x: x)
        self._children = dict.fromkeys([name for name,count in self._sort])

    def item(self, i):
        if not self._sort:
            self._scan()

        name, count = self._sort[i]
        if not self._children[name]:
            db_item = [x for x in self._db.query({'path': '/'.join(self.path+[name,])})]
            if len(db_item) == 1:
                self._children[name] = Asset(self._db, self, name, db_item[0])
            else:
                self._children[name] = Folder(self._db, self, name, i)
        return self._children[name]

    def count(self):
        if self._sort is None:
            self._scan()
        return len(self._sort)

class Asset:
    def __init__(self, db, parent, name, db_item):
        self._db = db
        self.name = name
        self.parent = parent
        self.path = parent.path + [name]
        self.idx = None
        self.db_item = db_item

    def count(self):
        return 0

class AssetFolderModel(QtCore.QAbstractItemModel):
    def __init__(self, parent, db):
        self._db = db
        QtCore.QAbstractItemModel.__init__(self, parent)
        self._root = Folder(self._db, None, [], 0)
        self._root.parent = self._root
        self._root.idx = QtCore.QModelIndex()
        self.setSupportedDragActions(Qt.LinkAction)

    def rowCount(self, idx):
        if idx.isValid():
            node = idx.internalPointer()
        else:
            node = self._root
        return node.count()

    def columnCount(self, idx):
        return 1

    def data(self, idx, role):
        assert idx.isValid()
        folder = idx.internalPointer()
        if role == Qt.DisplayRole:
            return QtCore.QVariant(folder.name)
        else:
            return None

    def flags(self, idx):
        node = idx.internalPointer()
        res = Qt.ItemIsEnabled | Qt.ItemIsSelectable
        if node.count() == 0:
            res |= Qt.ItemIsDragEnabled
        return res

    def mimeData(self, indexes):
        node = indexes[0].internalPointer()
        if hasattr(node, 'db_item'):
           mimeData = QtCore.QMimeData();
           bhfuse = config.get('BITHORDE', 'fusedir')
           f = os.path.join(bhfuse, node.db_item.magnetURL())
           mimeData.setUrls([QtCore.QUrl(f)])
           return mimeData
        else:
           return None

    def index(self, row, column, parent):
        if not self.hasIndex(row, column, parent):
            return QtCore.QModelIndex()

        if parent.isValid():
            parentItem = parent.internalPointer()
        else:
            parentItem = self._root

        # Find child
        obj = parentItem.item(row)

        # Cache Qt-Index
        if obj:
            return self.createIndex(row, column, obj)
        else:
            return QModelIndex()

    def parent(self, idx):
        if not idx.isValid():
            return QtCore.QModelIndex()
        child = idx.internalPointer()
        parent = child.parent
        assert child
        if (parent is self._root):
            return QtCore.QModelIndex()
        else:
            return self.createIndex(parent.row, 0, parent)

class FilterRule(QtGui.QWidget):
    onChanged = QtCore.pyqtSignal()

    def __init__(self, parent, db, keys):
        QtGui.QWidget.__init__(self, parent)
        self.db = db
        layout = self.layout = QtGui.QVBoxLayout(self)

        keybox = self.keybox = QtGui.QComboBox(self)
        keybox.addItem('- filter key -', userData=None)
        for key in keys:
            keybox.addItem(key, userData=key)
        keybox.currentIndexChanged.connect(self.onKeyChanged)
        layout.addWidget(keybox)

        valuebox = self.valuebox = QtGui.QComboBox(self)
        self.populateValuesForKey(None)
        valuebox.currentIndexChanged.connect(self.onChanged.emit)
        layout.addWidget(valuebox)

    def populateValuesForKey(self, key):
        self.valuebox.clear()
        self.valuebox.addItem('- is present -', userData=self.db.ANY)
        if key:
            for value in self.db.list_values(key):
                self.valuebox.addItem(value, userData=value)

    def onKeyChanged(self, idx):
        key = unicode(self.keybox.itemData(idx).toString())
        self.populateValuesForKey(key)
        self.onChanged.emit()

    def getRule(self):
        vb = self.valuebox
        value = vb.itemData(vb.currentIndex()).toPyObject()
        if isinstance(value, QtCore.QString):
            value = unicode(value)
        return self.getKey(), value

    def getKey(self):
        kb = self.keybox
        return unicode(kb.itemData(kb.currentIndex()).toString())

class FilterList(QtGui.QWidget):
    onChanged = QtCore.pyqtSignal()

    def __init__(self, parent, db):
        QtGui.QWidget.__init__(self, parent)
        self.db = db
        self.keys = [k for k,c in db.list_keys() if 1 < c < 32]
        layout = self.layout = QtGui.QVBoxLayout(self)
        layout.addWidget(QtGui.QLabel("Filters", self))
        layout.addStretch()
        self.addFilter()

    def addFilter(self):
        rule = FilterRule(self, self.db, self.keys)
        rule.onChanged.connect(self._onRuleChanged)
        self.layout.insertWidget(self.layout.count()-1, rule)

    def makeFilter(self):
        res = {}
        for c in self.children():
            if isinstance(c, FilterRule):
                k,v = c.getRule()
                if k:
                    res[k] = v
        return res

    def _onRuleChanged(self):
        empty = 0
        for c in self.children():
            if isinstance(c, FilterRule):
                if not c.getKey():
                    empty += 1
        if not empty:
            self.addFilter()
        self.onChanged.emit()

class PreviewWidget(QtGui.QFrame):
    def __init__(self, parent):
        QtGui.QFrame.__init__(self, parent)
        layout = QtGui.QVBoxLayout(self)
        self.setObjectName("preview")
        self.name = QtGui.QLabel(self)
        self.name.setObjectName("assetName")
        self.path = QtGui.QLabel(self)
        self.path.objectName = "assetPath"
        self.table = QtGui.QTableView(self)
        self.table.objectName = "attrtable"
        self.attrs = QtGui.QStandardItemModel(0, 2, self)
        self.table.setModel(self.attrs)
        layout.addWidget(self.name)
        layout.addWidget(self.path)
        layout.addItem(QtGui.QSpacerItem(20, 20, QtGui.QSizePolicy.Minimum, QtGui.QSizePolicy.Minimum))
        layout.addWidget(self.table)
        self.setLayout(layout)
        self.setStyleSheet(PREVIEW_STYLESHEET)

    def update(self, idx):
        item = idx.internalPointer()
        self.name.setText(item.name)
        self.path.setText("/"+"/".join(item.path[:-1]))
        asset = item.db_item
        self.attrs.clear()
        for k,v in sorted(asset.iteritems()):
            if k == u'path':
                continue
            for x in v:
                a = QtGui.QStandardItem(k)
                a.setSelectable(False)
                b = QtGui.QStandardItem(x)
                b.setSelectable(False)
                self.attrs.appendRow([a,b])

if __name__=='__main__':
    parser = OptionParser(usage="usage: %prog [options] <PATH>")
    parser.add_option("-d", "--dir", action="store_true", dest="dir",
                      help="dir-mode, list subdirectory")
    parser.add_option("-l", "--list", action="store_false", dest="dir",
                      help="list-mode, list files")

    (options, args) = parser.parse_args()
    if len(args)>1:
        parser.error("Only one path-argument supported")
    elif args:
        path=db.path_str2lst(args[0])
    else:
        path=[]

    thisdb = db.open(config)

    app = QtGui.QApplication(sys.argv)
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    mainwindow = uic.loadUi("browse.ui")
    mainwindow.show()

    def onFilterChanged():
        f = filter.makeFilter()
        print f
        print len([x for x in thisdb.query(f)])

    filter = FilterList(mainwindow, thisdb)
    filter.onChanged.connect(onFilterChanged)
    mainwindow.layout.insertWidget(0, filter)

    model = AssetFolderModel(mainwindow, thisdb)

    preview = PreviewWidget(None)

    mainwindow.columnView.setDragEnabled(True)
    mainwindow.columnView.setDragDropMode(QtGui.QAbstractItemView.DragOnly)
    mainwindow.columnView.setModel(model)
    mainwindow.columnView.updatePreviewWidget.connect(preview.update)
    mainwindow.columnView.setPreviewWidget(preview)
    sys.exit(app.exec_())

