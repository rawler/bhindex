import QtQuick 1.0

Item {
    anchors.fill: parent;

    Text {
        id: year
        text: (itemData.tags.path || "")
        elide: Text.ElideMiddle
        font.pointSize: 11
    }
}