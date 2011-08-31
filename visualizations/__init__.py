from PyQt4 import QtCore

class ItemVisualization(QtCore.QObject):
    def __init__(self, item):
        QtCore.QObject.__init__(self)
        self.item = item

    def title(self):
        return self.getTitle()
    def getTitle(self):
        return self.item['name'].any()
    titleChanged = QtCore.pyqtSignal()
    title = QtCore.pyqtProperty("QString", title, notify=titleChanged)

    def imageUri(self):
        return self.getImage()
    def getImage(self):
        if 'image' in self.item:
            return self.item['image'].any()
        else:
            return ""

    imageUriChanged = QtCore.pyqtSignal()
    imageUri = QtCore.pyqtProperty("QString", imageUri, notify=imageUriChanged)