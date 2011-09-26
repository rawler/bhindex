import QtQuick 1.0

ListView {
    clip: true
    model: myModel
    property color itemColor: Qt.rgba(0.9,0.9,0.9,0)
    property color hoverColor: Qt.rgba(1.,1.,1.,0.3)

    signal runAsset(variant asset)
    signal editAsset(variant asset)

    delegate: Rectangle {
        id: item
        height: imageUri ? itemImage.height : 50
        width: parent.width
        radius: 8
        MouseArea {
            id: mouse
            anchors.fill: parent
            hoverEnabled: true
            onClicked: {
                if (item.state == "selected")
                    item.state = "";
                else
                    item.state = "selected";
            }
        }
        color: mouse.containsMouse ? hoverColor : itemColor
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
        Column {
            id: itemButtons
            opacity: 0.0
            visible: false
            anchors.left: categoryIconView.left
            anchors.top: categoryIconView.bottom
            spacing: 2

            Button {
                id: playbutton
                width: 32
                height: 32

                icon: "open.png"
                onClicked: runAsset(obj)
            }
            Button {
                id: editbutton
                width: 32
                height: 32

                icon: "edit.png"
                onClicked: editAsset(obj)
            }
        }
        Text {
            id: shortPres
            anchors {
                left: categoryIconView.right
                right: itemImage.right
                top: parent.top
            }
            text: title
            y: 4
            elide: Text.ElideMiddle
            font.pointSize: 16
        }
        Image {
            id: itemImage
            source: imageUri
            height: 128
            anchors { right: parent.right; top: parent.top }
            fillMode: Image.PreserveAspectFit
        }
        Loader {
            id: briefView
            sourceComponent: obj.briefView()
            anchors {
                left: categoryIconView.right
                top: shortPres.bottom
                right: itemImage.left
            }
        }
        Loader {
            id: fullView
            anchors {
                left: categoryIconView.right
                leftMargin: 4
                top: briefView.bottom
                right: itemImage.left
            }
        }

        states: [
            State {
                name: "selected"
                PropertyChanges {target: item
                    height: Math.max(shortPres.height + briefView.height + fullView.height, itemImage.height)
                }
                PropertyChanges {target: fullView; sourceComponent: obj.fullView()}
                PropertyChanges {target: itemButtons
                    visible: true
                    opacity: 1.0
                }
            }
        ]

        transitions: Transition {
          PropertyAnimation { properties: "width,height"; duration: 250; easing.type: Easing.InOutQuad}
          PropertyAnimation { properties: "opacity"; duration: 250; easing.type: Easing.InOutQuad}
        }
    }
}
