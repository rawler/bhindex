import QtQuick 1.0

import ".."

Item {
    height: directors.height + actors.height

    PropertyDisplay {
        anchors { top: imdb_link.bottom; left: parent.left; right: parent.right }
        id: directors
        name: "Directors"
        text: (tags.director || "")
    }
    PropertyDisplay {
        id: actors
        anchors {
            top: directors.bottom
            left: parent.left
            right: parent.right
        }
        name: "Actors"
        text: (tags.actor || "")
    }
}
