import QtQuick 1.0

Text {
    font.pointSize: 11
    text: "Year: <b>" + (tags.year || "") + "</b> Genre: <b>" + (tags.genre || "") + "</b> Rating: <b>" + (tags.rating || "")
}
