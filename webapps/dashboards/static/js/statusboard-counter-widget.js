// JavaScript for a Status Board (http://panic.com/statusboard)
// widget that increments a big number.

var value;

$(function() {
    // For ease of local development, add ?desktop to the URL
    if (document.location.href.indexOf("desktop") !== -1)
        $("#container").css("backgroundColor", "black");
    updateData();
    var dataInterval = setInterval(updateData, 86400);
    var displayInterval = setInterval(updateDisplay, 1500);
});

var updateData = function() {
    var url = $("#value").data("source");
    $.get(url, function(data) {
        value = data.value;
    });
};

var updateDisplay = function() {
    // The closest we can get to a real-time count of exercises and videos done
    // today is by fetching that number from seven days ago and interpolating
    // based on the time of day.
    setTimeout(function() {
        var now = new Date();
        var todayMidnight = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0);
        var secsElapsedToday = (now - todayMidnight) / 1000;
        var percentDayComplete = secsElapsedToday / 86400;
        var displayValue = Math.floor(value * percentDayComplete);
        $("#value").text(commaSeparateNumber(displayValue));
    }, 1000 * Math.random());
};

// http://stackoverflow.com/a/12947816
var commaSeparateNumber = function(val) {
    while (/(\d+)(\d{3})/.test(val.toString())) {
        val = val.toString().replace(/(\d+)(\d{3})/, "$1" + "," + "$2");
    }
    return val;
};
