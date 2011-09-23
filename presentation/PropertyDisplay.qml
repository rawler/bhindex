import QtQuick 1.0

Item {
    property string name: ""
    property string text: ""

    height: textLabel.height

    Text {
        id: nameLabel
        font.pointSize: 11
        font.bold: true
        text: parent.name+":"
    }
    Text {
        id: textLabel
        anchors {
            left: nameLabel.right
            leftMargin: 4
            top: nameLabel.top
            right: parent.right
        }
        font.pointSize: 11
        wrapMode: Text.Wrap
        text: parent.text
    }
}