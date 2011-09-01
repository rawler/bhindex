from PySide import QtCore
import sys, os.path

class ItemVisualization(QtCore.QObject):
    def __init__(self, asset):
        QtCore.QObject.__init__(self)
        self.asset = asset

    def _title(self):
        return self.getTitle()
    def getTitle(self):
        return self.asset['name'].any()
    titleChanged = QtCore.Signal()
    title = QtCore.Property("QString", _title, notify=titleChanged)

    def _imageUri(self):
        return self.getImage()
    def getImage(self):
        if 'image' in self.asset:
            return self.asset['image'].any()
        else:
            return ""
    imageUriChanged = QtCore.Signal()
    imageUri = QtCore.Property("QString", _imageUri, notify=imageUriChanged)

    def _categoryIcon(self):
        dirname = os.path.dirname(sys.modules[type(self).__module__].__file__)
        fname = os.path.join(dirname, 'icon.png')
        if os.path.exists(fname):
            return fname
        else:
            return ""
    categoryIconChanged = QtCore.Signal()
    categoryIcon = QtCore.Property("QString", _categoryIcon, notify=categoryIconChanged)
