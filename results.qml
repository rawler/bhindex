import QtQuick 1.0

ListView {
    clip: true
    model: myModel
    property color itemColor: Qt.rgba(0.9,0.9,0.9,0)
    property color hoverColor: Qt.rgba(1.,1.,1.,0.3)

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
            onDoubleClicked: runAsset(obj)
        }
        color: mouse.containsMouse ?  hoverColor : itemColor
        property variant itemObj: obj
        Image {
            id: categoryIconView
            source: categoryIcon
            height: 32
            x: 10
            y: 4
            fillMode: Image.PreserveAspectFit
            smooth: true
        }
        Item {
            id: shortPres
            height: imageUri ? 128 : 50
            anchors { 
                left: categoryIconView.right
                right: parent.right
                verticalCenter: parent.verticalCenter
            }
            Text {
                id: titleView
                text: title
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
                source: imageUri
                anchors { right: parent.right; top: parent.top; bottom: parent.bottom }
                x: -10
                fillMode: Image.PreserveAspectFit
            }
            Loader {
                id: briefView
                sourceComponent: obj.briefView()
                anchors {
                    left: parent.left;
                    top: titleView.bottom
                    right: itemImage.left
                }
            }
        }
    }
}
