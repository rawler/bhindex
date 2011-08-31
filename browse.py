#!/usr/bin/env python
# -*- coding: utf-8 -*-

from PyQt4 import QtGui, uic, QtCore, QtDeclarative
from PyQt4.QtCore import Qt

import os, os.path as path, sys, time, signal
from ConfigParser import ConfigParser
from optparse import OptionParser
import subprocess

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config, magnet
config = config.read()

from visualizations import default, movies, series

VISUALIZATIONS = (series.Visualization, movies.Visualization, default.Visualization)

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

class FilterList(QtGui.QToolBar):
    onChanged = QtCore.pyqtSignal()

    def __init__(self, parent, db):
        QtGui.QToolBar.__init__(self, "Filter", parent)
        self.db = db
        self.keys = [k for k,c in db.list_keys()]
        self.addFilter()

    def addFilter(self):
        rule = FilterRule(self, self.db, self.keys)
        rule.onChanged.connect(self._onRuleChanged)
        self.addWidget(rule)

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
def fuseForAsset(asset):
    magnetUrl = magnet.fromDbObject(asset)
    return os.path.join(BHFUSE_MOUNT, magnetUrl)

def mapItemToView(item):
    for x in VISUALIZATIONS:
        if item.matches(x.CRITERIA):
            return x(item)
    assert False

class Results(QtDeclarative.QDeclarativeView):
    KEY_BLACKLIST = ('xt', 'path', 'filetype')
    def __init__(self, parent, db):
        QtDeclarative.QDeclarativeView.__init__(self, parent)
        self.db = db

        self.setResizeMode(self.SizeRootObjectToView)
        self.setAutoFillBackground(False)
        self.setStyleSheet("background:transparent;");
        self.rootContext().setContextProperty("myModel", [])
        self.setSource(QtCore.QUrl("results.qml"))

        self.dragStart = None

    def refresh(self, criteria):
        if criteria:
            assets = self.db.query(criteria)
        else:
            assets = self.db.all()

        self.model = model = list(mapItemToView(x) for x in assets)
        model.sort(key=lambda x: x.title)
        self.rootContext().setContextProperty("myModel", model)

    def onRun(self, idx):
        asset = self.model().itemFromIndex(idx).data(Qt.UserRole).toPyObject()
        subprocess.Popen(['xdg-open', fuseForAsset(asset)])

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.dragStart = event.pos()
        QtDeclarative.QDeclarativeView.mousePressEvent(self, event)

    def mouseMoveEvent(self, event):
        if self.dragStart and event.buttons() & Qt.LeftButton:
            distance = (event.pos() - self.dragStart).manhattanLength()
            if distance >= QtGui.QApplication.startDragDistance():
                item = self.itemAt(self.dragStart)
                self.dragStart = None
                obj = None

                while item:
                    obj = item.toGraphicsObject()
                    if obj:
                        obj = obj.property('itemData').toPyObject()
                        if obj:
                            break
                    item = item.parentItem()

                if obj:
                    drag = QtGui.QDrag(self);
                    mimeData = QtCore.QMimeData()
                    mimeData.setUrls([QtCore.QUrl(fuseForAsset(obj.asset))])
                    drag.setMimeData(mimeData)
                    dropAction = drag.exec_(Qt.CopyAction | Qt.LinkAction)
        QtDeclarative.QDeclarativeView.mouseMoveEvent(self, event)

if __name__=='__main__':
    parser = OptionParser(usage="usage: %prog [options] <PATH>")

    (options, args) = parser.parse_args()
    if len(args)>1:
        parser.error("Only one path-argument supported")
    elif args:
        path=db.path_str2lst(args[0])
    else:
        path=[]

    thisdb = db.open(config)

    QtGui.QApplication.setGraphicsSystem('raster')
    app = QtGui.QApplication(sys.argv)
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    mainwindow = QtGui.QMainWindow()
    mainwindow.resize(600,400)
    mainwindow.show()

    def onFilterChanged():
        f = filter.makeFilter()
        results.refresh(f)

    filter = FilterList(mainwindow, thisdb)
    filter.onChanged.connect(onFilterChanged)
    mainwindow.addToolBar(filter)

    results = Results(mainwindow, thisdb)

    results.refresh(None)
    results.show()

    mainwindow.setCentralWidget(results)

    #vlayout.addWidget(preview)
    sys.exit(app.exec_())

