from PySide import QtCore, QtGui

class FilterRule(QtGui.QWidget):
    onChanged = QtCore.Signal()

    def __init__(self, parent, db, keys):
        QtGui.QWidget.__init__(self, parent)
        self.db = db
        layout = self.layout = QtGui.QVBoxLayout(self)

        keybox = self.keybox = QtGui.QComboBox(self)
        keybox.addItem('- filter key -', userData=None)
        for key in keys:
            keybox.addItem(key, userData=key)
        layout.addWidget(keybox)
        keybox.currentIndexChanged.connect(self.onKeyChanged)

        valuebox = self.valuebox = QtGui.QComboBox(self)
        self.populateValuesForKey(None)
        layout.addWidget(valuebox)
        valuebox.currentIndexChanged.connect(self.onValueChanged)

    def populateValuesForKey(self, key):
        self.valuebox.clear()
        self.valuebox.addItem('- is present -', userData=self.db.ANY)
        if key:
            for value in self.db.list_values(key):
                self.valuebox.addItem(value, userData=value)

    @QtCore.Slot(unicode)
    def onKeyChanged(self, key):
        if isinstance(key, int):
            key = self.keybox.itemText(key)
        self.populateValuesForKey(key)

    @QtCore.Slot()
    def onValueChanged(self, value):
        if value is None:
            return
        self.onChanged.emit()

    def getRule(self):
        vb = self.valuebox
        value = vb.itemData(vb.currentIndex())
        return self.getKey(), value

    def getKey(self):
        kb = self.keybox
        return kb.itemData(kb.currentIndex())

class FilterList(QtGui.QToolBar):
    onChanged = QtCore.Signal()

    def __init__(self, parent, db):
        QtGui.QToolBar.__init__(self, "Filter", parent)
        self.db = db
        self.keys = [k for k,c in db.list_keys() if c>0]
        self.addFilter()

    def addFilter(self):
        rule = FilterRule(self, self.db, self.keys)
        rule.onChanged.connect(self._onRuleChanged)
        self.addWidget(rule)

    def criteria(self):
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
