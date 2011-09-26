import QtQuick 1.0

Rectangle {
    radius: 3
    signal clicked()
    property alias icon: i.source
    clip: true
    color: "#bbffffff"
    opacity: m.containsMouse ? 1.0: 0.9

    MouseArea {
        id: m
        anchors.fill: parent
        hoverEnabled: true
        onClicked: parent.clicked()
    }
    Image {
        anchors.fill: parent
        anchors.margins: 3
        id: i
        fillMode: Image.PreserveAspectFit
        smooth: true
    }
}