import QtQuick 1.0

Item {
    anchors.fill: parent;

    Text {
        id: year
        text: "Year: <b>" + (itemData.tags.year || "")
        font.pointSize: 11
    }

    Text {
        id: genre
        text: "Genre: <b>" + (itemData.tags.genre || "")
        font.pointSize: 11
        anchors {
            left: year.right
            leftMargin: 4
        }
    }

    Text {
        id: rating
        text: "Rating: <b>" + (itemData.tags.rating || "")
        font.pointSize: 11
        anchors {
            left: genre.right
            leftMargin: 4
        }
    }
}
