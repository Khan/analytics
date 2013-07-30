// JavaScript for a Status Board (http://panic.com/statusboard)
// widget that cycles through stories.

$(function() {
    // For ease of local developmentm add ?desktop to the URL
    if (document.location.href.indexOf("desktop") !== -1)
        $("#container").css("backgroundColor", "black");
    update();
    var updateInterval = setInterval(update, 60000);
});

var update = function() {
    var url = $("#value").data("source");
    $.get(url, function(data) {
        var story = _.chain(data.stories)
                        .filter(function(s) { return !!s.teaser; })
                        .shuffle()
                        .first()
                        .value();
        $("#title").text(story.name ? story.name + "'s story" : "Stories");
        $("#value").text(story.teaser).ellipsis({row: 6});
    });
};
