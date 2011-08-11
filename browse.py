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

import db, config, magnet
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
        self.keys = [k for k,c in db.list_keys()]
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

BHFUSE_MOUNT = config.get('BITHORDE', 'fusedir')
class AssetItemModel(QtGui.QStandardItemModel):
    def mimeData(self, indexes):
        node = indexes[0].internalPointer()
        mimeData = QtCore.QMimeData()
        urls = []
        for asset in set(self.itemFromIndex(idx).data(Qt.UserRole).toPyObject() for idx in indexes):
            magnetUrl = magnet.fromDbObject(asset)
            urls.append(QtCore.QUrl(os.path.join(BHFUSE_MOUNT, magnetUrl)))
        mimeData.setUrls(urls)
        return mimeData

class Results(QtGui.QTableView):
    def __init__(self, parent, db):
        QtGui.QTableView.__init__(self, parent)
        self.db = db

        self.setSortingEnabled(True)
        self.verticalHeader().hide()
        self.setSelectionBehavior(QtGui.QAbstractItemView.SelectRows)
        self.setSelectionMode(QtGui.QAbstractItemView.ExtendedSelection)
        self.setDragDropMode(QtGui.QAbstractItemView.DragOnly)

    def refresh(self, criteria):
        if criteria:
            assets = self.db.query(criteria)
        else:
            assets = self.db.all()

        model = AssetItemModel(0, 0, self)
        keys = [k for k,c in self.db.list_keys(criteria) if c > 1 and k not in ('xt')]

        bhfuse = config.get('BITHORDE', 'fusedir')
        model.clear()
        model.setHorizontalHeaderLabels(keys)

        for a in assets:
            def mkItem(k):
                #mimeData = QtCore.QMimeData();
                #f = os.path.join(bhfuse, node.db_item.magnetURL())
                #mimeData.setUrls([QtCore.QUrl(f)])
                #return mimeData
                label = key in a and u','.join(a[key]) or u''
                item = QtGui.QStandardItem(label)
                item.setEditable(False)
                item.setDropEnabled(False)
                item.setDragEnabled(True)
                item.setData(a, Qt.UserRole)
                return item
            row = [mkItem(key) for key in keys]
            model.appendRow(row)
        self.setModel(model)

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
        self.table.verticalHeader().hide()
        self.table.horizontalHeader().hide()
        self.attrs = QtGui.QStandardItemModel(0, 2, self)
        self.table.setModel(self.attrs)
        layout.addWidget(self.name)
        layout.addWidget(self.path)
        layout.addItem(QtGui.QSpacerItem(20, 20, QtGui.QSizePolicy.Minimum, QtGui.QSizePolicy.Minimum))
        layout.addWidget(self.table)
        self.setLayout(layout)
        self.setStyleSheet(PREVIEW_STYLESHEET)

    def update(self, item):
        self.name.setText('\n'.join(item['name']))
        self.path.setText('\n'.join(item['path']))
        self.attrs.clear()
        for k,v in sorted(item.iteritems()):
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

    mainwindow = QtGui.QWidget()
    mainwindow.show()
    layout = QtGui.QHBoxLayout(mainwindow)

    def onFilterChanged():
        f = filter.makeFilter()
        results.refresh(f)

    filter = FilterList(mainwindow, thisdb)
    filter.onChanged.connect(onFilterChanged)
    layout.insertWidget(0, filter)

    def onItemActivated(idx):
        preview.update(idx.data(Qt.UserRole).toPyObject())

    results = Results(mainwindow, thisdb)
    results.refresh(None)
    results.activated.connect(onItemActivated)
    layout.insertWidget(1, results)

    #model = AssetFolderModel(mainwindow, thisdb)

    preview = PreviewWidget(None)

    #mainwindow.columnView.setDragEnabled(True)
    #mainwindow.columnView.setDragDropMode(QtGui.QAbstractItemView.DragOnly)
    #mainwindow.columnView.setModel(model)
    #mainwindow.columnView.updatePreviewWidget.connect(preview.update)
    #mainwindow.columnView.setPreviewWidget(preview)
    layout.addWidget(preview)
    sys.exit(app.exec_())

