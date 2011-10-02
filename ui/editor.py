from PySide import QtCore, QtGui
from PySide.QtCore import Qt
from time import time

import db, scraper

class PropertyEditor(QtGui.QWidget):
    def __init__(self, parent, k, v):
        QtGui.QWidget.__init__(self, parent)
        self.key = k
        layout = QtGui.QHBoxLayout(self)
        layout.setContentsMargins(0,0,0,0)
        self.keyBox = QtGui.QLabel(k, self)
        layout.addWidget(self.keyBox)
        self.valueBox = QtGui.QLineEdit(v, self)
        layout.addWidget(self.valueBox)
        self.setFocusProxy(self.valueBox)

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

        self.scrollArea = QtGui.QScrollArea(self)
        self.items = []
        self.itemPane = QtGui.QWidget(self.scrollArea)
        self.itemLayout = QtGui.QVBoxLayout(self.itemPane)
        self.itemLayout.setSpacing(0)

        self.scrollArea.setWidgetResizable(True)
        self.scrollArea.setWidget(self.itemPane)

        layout.addWidget(self.scrollArea)
        buttons = QtGui.QDialogButtonBox(QtGui.QDialogButtonBox.Ok | QtGui.QDialogButtonBox.Cancel, parent=self)
        layout.addWidget(buttons)

        buttons.button(buttons.Ok).setDefault(True)
        self.addItemButton = buttons.addButton(self.tr("&Add"), buttons.ActionRole)
        self.addItemButton.clicked.connect(self.addKey)
        self.addItemButton.setToolTip(self.tr("Add a property. Leaving a property blank will remove it."))
        self.scrapeButton = buttons.addButton(self.tr("Auto-&Fill"), buttons.ActionRole)
        self.scrapeButton.clicked.connect(self.scrape)
        self.scrapeButton.setToolTip(self.tr("Try to auto-fetch properties from remote data-sources."))
        buttons.accepted.connect(self.onOK)
        buttons.rejected.connect(self.close)

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
        item = PropertyEditor(self.itemPane, k, v)
        self.itemLayout.addWidget(item)
        self.items.append(item)
        return item

    def addKey(self):
        key, ok = QtGui.QInputDialog.getText(self, self.tr("New Key Name"),
                                     self.tr("New Key Name"), QtGui.QLineEdit.Normal,
                                     "", Qt.Popup)
        if key and ok:
            item = self._addItem(key, "")
            app = QtGui.QApplication
            while app.hasPendingEvents():
                app.processEvents()
            QtGui.QApplication.processEvents()
            self.scrollArea.ensureWidgetVisible(item, 50, 50)
            item.setFocus()

    def onOK(self):
        self.save()
        self.close()

    def _updateAsset(self):
        new_obj = dict()
        t = time()
        for item in self.items:
            k = item.key
            v = item.value()
            if not v:
                continue
            if k in new_obj:
                new_obj[k].add(v)
            else:
                new_obj[k] = db.ValueSet([v], t=t)
        obj = self.asset
        for k,v in new_obj.iteritems():
            obj[k] = v
        for k,v in obj.iteritems():
            if k not in new_obj:
                del obj[k]

    def save(self):
        self._updateAsset()
        self.db.update(self.asset)
        self.db.commit()

    def scrape(self):
        self._updateAsset()
        if scraper.scrape_for(self.asset):
            self.db.update(self.asset)
        else:
            print "Scraping failed"
        self._reload()

    def close(self):
        self.uiitem.setAsset(self.db[self.asset.id])
        self.model.signalChanged(self.uiitem)
        QtGui.QWidget.close(self)