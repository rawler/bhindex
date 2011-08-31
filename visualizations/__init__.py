from PyQt4 import QtCore

class ItemVisualization(QtCore.QObject):
    def __init__(self, asset):
        QtCore.QObject.__init__(self)
        self.asset = asset

    def title(self):
        return self.getTitle()
    def getTitle(self):
        return self.asset['name'].any()
    titleChanged = QtCore.pyqtSignal()
    title = QtCore.pyqtProperty("QString", title, notify=titleChanged)

    def imageUri(self):
        return self.getImage()
    def getImage(self):
        if 'image' in self.asset:
            return self.asset['image'].any()
        else:
            return ""

    imageUriChanged = QtCore.pyqtSignal()
    imageUri = QtCore.pyqtProperty("QString", imageUri, notify=imageUriChanged)