import QtQuick 1.0

import ".."

Column {
    PropertyDisplay {
        anchors { left: parent.left; right: parent.right }
        name: "Plot"
        text: (tags.plot || "")
    }
    PropertyDisplay {
        anchors { left: parent.left; right: parent.right }
        name: "Directors"
        text: (tags.director || "")
    }
    PropertyDisplay {
        anchors { left: parent.left; right: parent.right }
        name: "Actors"
        text: (tags.actor || "")
    }
}
