$(function() {


// TODO(david): @spicyj's (or eater?) idea: display last GitHub issue somewhere
// TODO(david): Use maxmind database
var IP_INFO_API_URL = "http://api.ipinfodb.com/v3/ip-city/?callback=?";
var IP_INFO_API_KEY =
        "ea615758ce2f4b49b9ab4242b2159fcfc72860859bbce72749f75bfe1df98242";
var MAX_MARKERS_ON_SCREEN = 1200;
var POLL_INTERVAL_MS = 1000;


var localStorage = window.localStorage || {};

var mapOptions = {
    center: new google.maps.LatLng(localStorage["gmapsCenterLat"] || 0,
                                   localStorage["gmapsCenterLng"] || 0),
    zoom: +localStorage["gmapsZoom"] || 2,
    mapTypeId: localStorage["gmapsMapTypeId"] || google.maps.MapTypeId.ROADMAP
};

var attemptTemplate = Handlebars.compile($("#attempt-entry").html());
var map = new google.maps.Map($("#map")[0], mapOptions);
var prevFloat = null;
var markers = [];


google.maps.event.addListener(map, "center_changed", function() {
    localStorage["gmapsCenterLat"] = map.getCenter().lat();
    localStorage["gmapsCenterLng"] = map.getCenter().lng();
});

google.maps.event.addListener(map, "zoom_changed", function() {
    localStorage["gmapsZoom"] = map.getZoom();
});

google.maps.event.addListener(map, "maptypeid_changed", function() {
    localStorage["gmapsMapTypeId"] = map.getMapTypeId();
});


/**
 * Converts exercise ID to pretty display name.
 * @param {string} exid Exercise ID (name).
 * @return {string} Pretty display name.
 */
var getExerciseName = (function() {
    var displayNameMap = {};

    // Use the KA API to get pretty display names for exercises
    $(function() {
        $.getJSON("http://www.khanacademy.org/api/v1/exercises",
                function(exercises) {
            _.each(exercises, function(ex) {
                displayNameMap[ex.name] = ex.display_name;
            });
        });
    });

    return function(exid) {
        return displayNameMap[exid] || exid;
    };
})();


/**
 * Creates a Google Maps marker.
 * @param {string} pinColor Hex code of the desired marker color, eg. "FE7569".
 * @param {Object} options Additional options to pass to the marker constructor.
 * @return {google.map.Marker} The marker.
 */
var makeMarker = function(pinColor, options) {
    // From http://stackoverflow.com/questions/7095574
    var pinImage = new google.maps.MarkerImage("http://chart.apis.google.com/" +
            "chart?chst=d_map_pin_letter&chld=%E2%80%A2|" + pinColor,
        new google.maps.Size(21, 34),
        new google.maps.Point(0, 0),
        new google.maps.Point(10, 34));
    var pinShadow = new google.maps.MarkerImage(
            "http://chart.apis.google.com/chart?chst=d_map_pin_shadow",
        new google.maps.Size(40, 37),
        new google.maps.Point(0, 0),
        new google.maps.Point(12, 35));

    return new google.maps.Marker(_.extend({
        icon: pinImage,
        shadow: pinShadow
    }, options));
};


window.setInterval(function() {

    // TODO(david): Eventually add other types of activity, such as videos
    $.getJSON("http://www.khanacademy.org/exercisestats/recentproblemlog",
            function(problemlog) {

        // Don't show again if last problem log was just shown
        if (problemlog.randomFloat === prevFloat) {
            return;
        }
        prevFloat = problemlog.randomFloat;

        var params = {
            key: IP_INFO_API_KEY,
            format: "json",
            ip: problemlog.ipAddress
        };

        $.getJSON(IP_INFO_API_URL, params, function(data) {

            var color = problemlog.correct ? "79A94E" : "FE7569";

            // Only draw map marker if we got back lat lng and it's not (0, 0)
            if (+data.latitude || +data.longitude) {

                markers.push(makeMarker(color, {
                    position: new google.maps.LatLng(data.latitude,
                        data.longitude),
                    map: map,
                    animation: google.maps.Animation.DROP
                }));

                if (markers.length > MAX_MARKERS_ON_SCREEN) {
                    var marker = markers.shift();
                    marker.setMap(null);  // Remove this marker from map
                }

            }

            var rawAttempt = problemlog.attempts[0],
                answer = rawAttempt,
                useMathjax = (rawAttempt && rawAttempt.substr(0, 6) ===
                        '"<code' && window.MathJax);
            if (problemlog.countAttempts > 0 && problemlog.countHints === 0) {
                try {
                    // Try to parse out text from attempt contents, which may be
                    // a number as a string, HTML, an array, or who knows what
                    // TODO(david): Parse MathJax
                    answer = JSON.parse(answer);
                    answer = $(answer).text() || answer;
                    if (useMathjax) {
                        answer = "\\(" + answer + "\\)";
                    }
                } catch (e) {}
            } else {
                if (problemlog.countHints > 0) {
                    answer = "using hints";
                } else {
                    answer = "[see console]";
                    console.log(problemlog);
                }
            }

            var $attempt = $(attemptTemplate(_.extend(problemlog, {
                color: color,
                answer: String(answer),
                exerciseDisplayName: getExerciseName(problemlog.exercise)
            })));

            $("#stats-text")
                .append($attempt)
                .scrollTop($("#stats-text")[0].scrollHeight);

            if (useMathjax) {
                MathJax.Hub.Queue(["Typeset", MathJax.Hub, $attempt[0]]);
            }

        });

    });

}, POLL_INTERVAL_MS);

});
