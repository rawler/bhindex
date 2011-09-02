import QtQuick 1.0

Text {
    text: itemData.tags.year
    y: 30
    elide: Text.ElideMiddle
    font.pointSize: 12
    anchors { 
        left: parent.left
        right: parent.right
    }
}
