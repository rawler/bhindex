import QtQuick 1.0

Item {
    anchors.fill: parent;

    Text {
        id: year
        text: "Year: <b>" + (tags.year || "")
        font.pointSize: 11
    }

    Text {
        id: genre
        text: "Genre: <b>" + (tags.genre || "")
        font.pointSize: 11
        anchors {
            left: year.right
            leftMargin: 4
        }
    }

    Text {
        id: rating
        text: "Rating: <b>" + (tags.rating || "")
        font.pointSize: 11
        anchors {
            left: genre.right
            leftMargin: 4
        }
    }
}
