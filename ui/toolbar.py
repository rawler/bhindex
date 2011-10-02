from PySide import QtCore, QtGui

class SortSelection(QtGui.QGroupBox):
    SORT_TIME = "time"
    SORT_TITLE = "title"

    def __init__(self, parent):
        QtGui.QGroupBox.__init__(self, parent)
        layout = QtGui.QVBoxLayout()
        self.timeBtn = QtGui.QRadioButton("Sort by Time", self)
        self.timeBtn.setToolTip("Order items based on last change of item.")
        layout.addWidget(self.timeBtn)
        self.titleBtn = QtGui.QRadioButton("Sort by Title", self)
        self.titleBtn.setToolTip("Order items based on estimated title. <b>NOTE:</b> this is not 100% accurate.")
        layout.addWidget(self.titleBtn)
        self.setLayout(layout)
        self.titleBtn.toggled.connect(self._emit_Changed)
        self.timeBtn.toggled.connect(self._emit_Changed)
        self.timeBtn.setChecked(True)

    @QtCore.Slot()
    def _emit_Changed(self):
        btn = self.sender()
        if btn == self.timeBtn:
            self._sortKey = self.SORT_TIME
        elif btn == self.titleBtn:
            self._sortKey = self.SORT_TITLE
        else:
            print "ERROR: Neither sort order seems to be checked."
        self.sortKeyChanged.emit(self._sortKey)

    def _getSortKey(self):
        return self._sortKey

    sortKeyChanged = QtCore.Signal(str)
    sortKey = QtCore.Property(str, _getSortKey, notify=sortKeyChanged)

class MainToolbar(QtGui.QToolBar):
    def __init__(self, parent):
        QtGui.QToolBar.__init__(self, parent)

        self.setMaximumSize(200, 200)
        self.sort = sort = SortSelection(self)
        #self.sort.show()
        self.addWidget(sort)
