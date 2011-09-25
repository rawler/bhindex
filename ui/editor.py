from PySide import QtCore, QtGui
from time import time

import db, scraper

class PropertyEditor(QtGui.QWidget):
    def __init__(self, parent, k, v):
        QtGui.QWidget.__init__(self, parent)
        self.key = k
        layout = QtGui.QHBoxLayout(self)
        self.keyBox = QtGui.QLabel(k, self)
        layout.addWidget(self.keyBox)
        self.valueBox = QtGui.QLineEdit(v, self)
        layout.addWidget(self.valueBox)

    def value(self):
        return self.valueBox.text()

class ItemEditor(QtGui.QDialog):
    def __init__(self, parent, db, model, uiitem):
        QtGui.QDialog.__init__(self, parent)
        self.db = db
        self.model = model
        self.uiitem = uiitem
        self.asset = uiitem.asset
        self.setWindowTitle("%s (%s)" % (uiitem.title, uiitem.asset.id))
        layout = QtGui.QVBoxLayout(self)
        self.itemLayout = QtGui.QVBoxLayout()
        self.items = []
        layout.addLayout(self.itemLayout)
        layout.addStretch(2)
        buttons = QtGui.QDialogButtonBox(QtGui.QDialogButtonBox.Ok | QtGui.QDialogButtonBox.Cancel, parent=self)
        layout.addWidget(buttons)

        self.addItemButton = buttons.addButton("Add Key", buttons.ActionRole)
        self.addItemButton.clicked.connect(self.addKey)
        self.scrapeButton = buttons.addButton("Scrape Remote", buttons.ActionRole)
        self.scrapeButton.clicked.connect(self.scrape)
        buttons.button(QtGui.QDialogButtonBox.Ok).clicked.connect(self.onOK)
        buttons.button(QtGui.QDialogButtonBox.Cancel).clicked.connect(self.close)

        self._reload()
        self.show()

    def _reload(self):
        for item in self.items:
            self.itemLayout.removeWidget(item)
            item.deleteLater()

        self.items = []
        for k, values in sorted(self.asset.iteritems()):
            for v in values:
                self._addItem(k, v)

    def _addItem(self, k, v):
        item = PropertyEditor(self, k, v)
        self.itemLayout.addWidget(item)
        self.items.append(item)

    def addKey(self):
        key, ok = QtGui.QInputDialog.getText(self, self.tr("New Key Name"),
                                     self.tr("New Key Name"), QtGui.QLineEdit.Normal,
                                     "")
        if key and ok:
            self._addItem(key, "")

    def onOK(self):
        self.save()
        self.close()

    def save(self):
        new_obj = dict()
        t = time()
        for item in self.items:
            k = item.key
            if k in new_obj:
                new_obj[k].add(item.value())
            else:
                new_obj[k] = db.ValueSet([item.value()], t=t)
        obj = self.asset
        for k,v in new_obj.iteritems():
            obj[k] = v
        for k,v in obj.iteritems():
            if k not in new_obj:
                del obj[k]
        self.db.update(obj)
        self.db.commit()

    def scrape(self):
        self.save()
        if scraper.scrape_for(self.asset):
            self.db.update(self.asset)
        self._reload()

    def close(self):
        self.uiitem.setAsset(self.db[self.asset.id])
        self.model.signalChanged(self.uiitem)
        QtGui.QWidget.close(self)