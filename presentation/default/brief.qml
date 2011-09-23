import QtQuick 1.0

Text {
    text: tags.path || ""
    elide: Text.ElideMiddle
    font.pointSize: 11
}