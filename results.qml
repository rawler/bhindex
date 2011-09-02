import QtQuick 1.0

ListView {
    clip: true
    model: myModel
    property color itemColor: Qt.rgba(0.9,0.9,0.9,0)
    property color hoverColor: Qt.rgba(0.9,0.9,0.9,0.3)

    signal runAsset(variant asset)

    delegate: Rectangle {
        id: item
        height: shortPres.height+4
        width: parent.width
        radius: 8
        MouseArea {
            id: mouse
            anchors.fill: parent
            hoverEnabled: true
            onDoubleClicked: runAsset(itemData)
        }
        color: mouse.containsMouse ?  hoverColor : itemColor
        property variant itemData: modelData
        Image {
            id: categoryIcon
            source: modelData.categoryIcon
            height: 32
            x: 10
            y: 4
            fillMode: Image.PreserveAspectFit
            smooth: true
        }
        Item {
            id: shortPres
            height: modelData.imageUri ? 128 : 40
            anchors { 
                left: categoryIcon.right
                right: parent.right
                verticalCenter: parent.verticalCenter
            }
            Text {
                text: modelData.title
                y: 4
                elide: Text.ElideMiddle
                font.pointSize: 16
                anchors { 
                    left: parent.left
                    right: itemImage.left
                }
            }
            Image {
                id: itemImage
                source: modelData.imageUri
                anchors { right: parent.right; top: parent.top; bottom: parent.bottom }
                x: -10
                fillMode: Image.PreserveAspectFit
            }
            Loader {
                id: briefView
                sourceComponent: itemData.briefView()
                anchors.fill: parent
            }
        }
    }
}
