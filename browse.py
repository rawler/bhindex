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
            if self._db.by_name('/'.join(self.path+[name,])):
                self._children[name] = Asset(self._db, self, name)
            else:
                self._children[name] = Folder(self._db, self, name, i)
        return self._children[name]

    def count(self):
        if not self._sort:
            self._scan()
        return len(self._sort)

class Asset:
    def __init__(self, db, parent, name):
        self._db = db
        self.name = name
        self.parent = parent
        self.path = parent.path + [name]
        self.idx = None

        print self._db.by_name('/'.join(self.path))

    def count(self):
        return 0

class AssetFolderModel(QtCore.QAbstractItemModel):
    def __init__(self, parent, db):
        self._db = db
        QtCore.QAbstractItemModel.__init__(self, parent)
        self._root = Folder(self._db, None, [], 0)
        self._root.parent = self._root
        self._root.idx = QtCore.QModelIndex()

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
        return Qt.ItemIsEnabled or Qt.ItemIsSelectable

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

class PreviewWidget(QtGui.QWidget):
    def __init__(self, parent):
        QtGui.QWidget.__init__(self, parent)
        layout = QtGui.QVBoxLayout(self)
        self.name = QtGui.QLabel(self)
        self.path = QtGui.QLabel(self)
        layout.addWidget(self.name)
        layout.addWidget(self.path)
        layout.addItem(QtGui.QSpacerItem(20, 259, QtGui.QSizePolicy.Minimum, QtGui.QSizePolicy.Expanding))
        self.setLayout(layout)

    def update(self, idx):
        item = idx.internalPointer()
        self.name.setText(item.name)
        self.path.setText("/"+"/".join(item.path[:-1]))

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
    model = AssetFolderModel(mainwindow, thisdb)

    preview = PreviewWidget(None)

    mainwindow.columnView.setModel(model)
    mainwindow.columnView.updatePreviewWidget.connect(preview.update)
    mainwindow.columnView.setPreviewWidget(preview)
    sys.exit(app.exec_())

