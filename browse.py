#!/usr/bin/env python
# -*- coding: utf-8 -*-

from PyQt4 import QtGui, uic, QtCore
from PyQt4.QtCore import Qt

import os, os.path as path, sys, time
from ConfigParser import ConfigParser
from optparse import OptionParser
import subprocess

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config
config = config.read()

class Folder:
    def __init__(self, db, parent, name):
        self._db = db
        self.name = name
        self.parent = parent
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

        name = self._sort[i][0]
        if not self._children[name]:
            if self._sort[i][1] > 0:
                self._children[name] = Folder(self._db, self, name)
            else:
                self._children[name] = Asset(self._db, self, name)
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

    def count(self):
        return 0

import traceback

class AssetFolderModel(QtCore.QAbstractItemModel):
    def __init__(self, parent, db):
        self._db = db
        QtCore.QAbstractItemModel.__init__(self, parent)
        self._root = Folder(self._db, None, [])

    def rowCount(self, idx):
        if idx.isValid():
            folder = idx.internalPointer()
        else:
            folder = self._root
        return folder.count()

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
        if parent.isValid():
            folder = parent.internalPointer()
        else:
            folder = self._root

        # Find child
        obj = folder.item(row)

        # Cache Qt-Index
        if not obj.idx:
            obj.idx = self.createIndex(row, column, obj)

        return obj.idx

    def parent(self, idx):
        assert idx.isValid()
        folder = idx.internalPointer()
        assert folder
        assert folder.idx
        if folder.parent is not self._root:
            return folder.parent.idx
        else:
            return QtCore.QModelIndex()

    def path(self):
        return self._path

def populate(thisdb, list, root=[]):
    for folder, count in thisdb.dir("path", root).iteritems():
        item = FolderItem(list, folder)
        if count > 1:
            populate(thisdb, item, root+[folder])
        list.appendRow(item)

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
    mainwindow = uic.loadUi("browse.ui")
    mainwindow.show()
    model = AssetFolderModel(mainwindow, thisdb)

    mainwindow.columnView.setModel(model)
    app.exec_()

