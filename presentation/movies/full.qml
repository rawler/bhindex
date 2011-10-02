import QtQuick 1.0

import ".."

Column {
    PropertyDisplay {
        anchors { left: parent.left; right: parent.right }
        id: country
        name: "Country"
        text: (tags.country || "")
    }
    PropertyDisplay {
        anchors { left: parent.left; right: parent.right }
        id: plot
        name: "Plot"
        text: (tags.plot || "")
    }
    PropertyDisplay {
        anchors { left: parent.left; right: parent.right }
        id: directors
        name: "Directors"
        text: (tags.director || "")
    }
    PropertyDisplay {
        anchors { left: parent.left; right: parent.right }
        id: actors
        name: "Actors"
        text: (tags.actor || "")
    }
}
