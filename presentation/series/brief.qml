import QtQuick 1.0

Text {
    text: "Genre: <b>" + (tags.genre || "") + "</b> Rating: <b>" + (tags.episode_rating || "")
    font.pointSize: 11
}
