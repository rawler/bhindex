import os, subprocess

from PySide import QtCore, QtDeclarative, QtGui
from PySide.QtCore import Qt

import config, magnet
config = config.read()

from bithorde import QueryThread
bithorde_querier = QueryThread()

from presentation import default, movies, series

PRESENTATIONS = (series.Presentation, movies.Presentation, default.Presentation)

BHFUSE_MOUNT = config.get('BITHORDE', 'fusedir')
def fuseForAsset(asset):
    magnetUrl = magnet.fromDbObject(asset)
    return os.path.join(BHFUSE_MOUNT, magnetUrl)

def mapItemToView(item):
    for x in PRESENTATIONS:
        if item.matches(x.CRITERIA):
            return x(item)
    assert False

class ResultList(QtCore.QAbstractListModel):
    ObjRole = Qt.UserRole
    TagsRole = Qt.UserRole + 1
    ImageURIRole = Qt.UserRole + 2

    def __init__(self, parent, results):
        self._unfiltered = iter(results)
        self._list = list()
        QtCore.QAbstractListModel.__init__(self, parent)
        self.setRoleNames({
            Qt.DisplayRole: "title",
            Qt.DecorationRole: "categoryIcon",
            self.TagsRole: "tags",
            self.ImageURIRole: "imageUri",
            self.ObjRole: "obj",
        })

    def canFetchMore(self, _):
        return bool(self._unfiltered)

    def fetchMore(self, _):
        i = 0
        for asset in self._unfiltered:
            id = asset.get('xt', '')
            id = id and id.any()
            if not id.startswith('tree:tiger:'):
                continue
            bithorde_querier(id[len('tree:tiger:'):], self._queueAppend, asset)
            i += 1
            if i > 15: break

    def _queueAppend(self, db_asset):
        event = QtCore.QEvent(QtCore.QEvent.User)
        event.asset = db_asset
        QtCore.QCoreApplication.postEvent(self, event)

    def event(self, event):
        if event.type()==QtCore.QEvent.User and hasattr(event, 'asset'):
            self._append(mapItemToView(event.asset))
            return True
        return QtDeclarative.QDeclarativeView.event(self, event)

    def _append(self, val):
        pos = len(self._list)
        self.beginInsertRows(QtCore.QModelIndex(), pos, pos)
        self._list.append(val)
        self.endInsertRows()

    def rowCount(self, _):
        return len(self._list)

    def data(self, idx, role):
        obj = self._list[idx.row()]
        if role == Qt.DisplayRole:
            return obj.title
        if role == Qt.DecorationRole:
            return obj.categoryIcon
        if role == self.TagsRole:
            return obj.tags
        if role == self.ImageURIRole:
            return obj.imageUri
        if role == self.ObjRole:
            return obj
        return obj.title

class ResultsView(QtDeclarative.QDeclarativeView):
    KEY_BLACKLIST = ('xt', 'path', 'filetype')
    def __init__(self, parent, db):
        QtDeclarative.QDeclarativeView.__init__(self, parent)
        self.db = db

        self.setResizeMode(self.SizeRootObjectToView)
        self.setAutoFillBackground(False)
        self.setStyleSheet("background:transparent;");
        self.rootContext().setContextProperty("myModel", [])
        self.setSource(QtCore.QUrl("results.qml"))

        for vis in PRESENTATIONS:
            vis.loadComponents(self.engine())

        self.rootObject().runAsset.connect(self.runAsset)
        self.dragStart = None

    def refresh(self, criteria):
        if criteria:
            assets = self.db.query(criteria)
        else:
            assets = self.db.all()

        def sort_key(x):
            title = x.get('title')
            title = title and title.any()
            return title or x['name'].any()
        assets = sorted(assets, key=sort_key)

        bithorde_querier.clear()
        self.model = model = ResultList(self, assets)
        self.rootContext().setContextProperty("myModel", model)

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
                    if hasattr(item, 'property'):
                        obj = item.property('itemObj')
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

    def runAsset(self, guiitem):
        asset = guiitem.asset
        subprocess.Popen(['xdg-open', fuseForAsset(asset)])
