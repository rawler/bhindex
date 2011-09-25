from PySide import QtCore, QtDeclarative
import sys, os.path

class ItemPresentation(QtCore.QObject):
    def __init__(self, asset):
        QtCore.QObject.__init__(self)
        self.setAsset(asset)

    def setAsset(self, asset):
        self.asset = asset
        self.__tags = None
        self.tagsChanged.emit()
        self.titleChanged.emit()
        self.imageUriChanged.emit()

    def _tags(self):
        if not self.__tags:
            self.__tags = tags = QtDeclarative.QDeclarativePropertyMap()
            for k,v in self.asset.iteritems():
                tags.insert(k, v.join())
        return self.__tags
    tagsChanged = QtCore.Signal()
    tags = QtCore.Property(QtCore.QObject, _tags, notify=tagsChanged)

    def _title(self):
        return self.getTitle()
    def getTitle(self):
        return self.asset['name'].any()
    titleChanged = QtCore.Signal()
    title = QtCore.Property(unicode, _title, notify=titleChanged)

    def _imageUri(self):
        return self.getImage()
    def getImage(self):
        if 'image' in self.asset:
            return self.asset['image'].any()
        else:
            return ""
    imageUriChanged = QtCore.Signal()
    imageUri = QtCore.Property(unicode, _imageUri, notify=imageUriChanged)

    def _categoryIcon(self):
        dirname = os.path.dirname(sys.modules[type(self).__module__].__file__)
        fname = os.path.join(dirname, 'icon.png')
        if os.path.exists(fname):
            return fname
        else:
            return ""
    categoryIconChanged = QtCore.Signal()
    categoryIcon = QtCore.Property(unicode, _categoryIcon, notify=categoryIconChanged)

    @QtCore.Slot(result=QtDeclarative.QDeclarativeComponent)
    def briefView(self):
        return self._briefView

    @QtCore.Slot(result=QtDeclarative.QDeclarativeComponent)
    def fullView(self):
        return self._fullView

    @classmethod
    def loadComponents(cls, engine):
        dirname = os.path.dirname(sys.modules[cls.__module__].__file__)
        briefSrc = os.path.join(dirname, 'brief.qml')
        if os.path.exists(briefSrc):
            cls._briefView = QtDeclarative.QDeclarativeComponent(engine, briefSrc)
        else:
            cls._briefView = None
        fullSrc = os.path.join(dirname, 'full.qml')
        if os.path.exists(fullSrc):
            cls._fullView = QtDeclarative.QDeclarativeComponent(engine, fullSrc)
        else:
            cls._fullView = None
