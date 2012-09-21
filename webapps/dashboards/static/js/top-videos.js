/**
 * Logic for fetching data and rendering the daily video
 * statistics dashboard.
 */

(function() {
/**
 * Entry point - called on DOMReady event.
 */
var init = function() {
    // Note that JS Date implementation does "the right thing" even if
    // today is the first of the month.
    var lastMonth = new Date();
    lastMonth.setMonth(lastMonth.getMonth() - 1);
    lastMonth.setDate(1);

    $("#top-video-date")
        .datepicker({ dateFormat: "yy-mm-dd" })
        .datepicker("setDate", lastMonth);
    $("#top-video-date").change(refreshTopVideoSummary);
    refreshTopVideoSummary();
};

/**
 * Refresh the top video table
 */
var refreshTopVideoSummary = function() {
    var url = "/data/top_videos?";
    var startDate = $("#top-video-date").val();
    var timeScale = $("#top-video-timescale").val();
    var params = {
        "start_date": startDate,
        "time_scale": timeScale,
    };
    var showTitle = "Top Videos for the " + timeScale + " of " + startDate;
    $("#top-video-title").html(showTitle);
    $("#top-video-table-container").text("Loading...");
    AjaxCache.getJson(url, params, handleDataLoadForTopVideos);
};

/**
 * Handles the raw JSON data returned from the server for the
 * top videos
 */
var handleDataLoadForTopVideos = function(data) {
    var results = data["results"];
    renderTopVideoSummary(results, $("#top-video-table-container"));
};

// TODO(benkomalo): have a configurable sort
/**
 * Renders the top video table
 * Each row is a record summarizing the stats for a video
 * container is the div where we will show the results
 * (e.g. how many people completed it).
 */
var renderTopVideoSummary = function(jsonRows, container) {
    if (!(jsonRows && jsonRows.length)) {
        container.html("<strong>No data for that date :(</strong>");
        return;
    }
    var tableTemplate = Handlebars.compile($("#top-video-table").text());
    var table = $(tableTemplate());
    var rowTemplate = Handlebars.compile($("#top-video-row-template").text());
    _.chain(jsonRows)
        .sortBy(function(row) { return -row["hours_all"]; })
        .first(200)
        .each(function(row) {
            row["href"] = "/video-summary?title=" + 
                encodeURIComponent(row["title"]);
            $(rowTemplate(row)).appendTo(table);
         });

    container.html("");
    container.append(table);
};

$(document).ready(function() {
    init();
});


})();
