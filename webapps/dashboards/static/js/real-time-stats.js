$(function() {


var IP_INFO_API_URL = "http://api.ipinfodb.com/v3/ip-city/?callback=?";
var IP_INFO_API_KEY =
        "ea615758ce2f4b49b9ab4242b2159fcfc72860859bbce72749f75bfe1df98242";
var MAX_MARKERS_ON_SCREEN = 200;
var POLL_INTERVAL_MS = 1000;


var mapOptions = {
    center: new google.maps.LatLng(0, 0),
    zoom: 2,
    mapTypeId: google.maps.MapTypeId.ROADMAP
};

var attemptTemplate = Handlebars.compile($("#attempt-entry").html());
var map = new google.maps.Map($("#map")[0], mapOptions);
var prevFloat = null;
var markers = [];


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
        new google.maps.Point(0,0),
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

            if (data.latitude === "0" && data.longitude === "0") {
                // Probably an invalid IP address... skip
                return;
            }

            var color = problemlog.correct ? "79A94E" : "FE7569";
            markers.push(makeMarker(color, {
                position: new google.maps.LatLng(data.latitude, data.longitude),
                map: map,
                animation: google.maps.Animation.DROP
            }));

            $("#stats-text")
                .append(attemptTemplate(_.extend(problemlog, {
                    color: color,
                    answer: problemlog.attempts[0],
                    exerciseDisplayName: getExerciseName(problemlog.exercise)
                })))
                .scrollTop($("#stats-text")[0].scrollHeight);

            if (markers.length > MAX_MARKERS_ON_SCREEN) {
                var marker = markers.shift();
                marker.setMap(null);  // Remove this marker from map
            }

        });

    });

}, POLL_INTERVAL_MS);

});
