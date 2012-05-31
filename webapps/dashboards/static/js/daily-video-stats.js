/**
 * Logic for fetching data and rendering the daily video
 * statistics dashboard.
 */

// Namespace
var VideoStats = {};

/**
 * Entry point - called on DOMReady event.
 */
VideoStats.init = function() {
    // Note that JS Date implementation does "the right thing" even if
    // today is the first of the month.
    var yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);

    $("#datestamp")
        .datepicker({ dateFormat: "yy-mm-dd" })
        .datepicker("setDate", yesterday);
    $("#datestamp").change(VideoStats.refresh);
    $("#user_category").change(VideoStats.refresh);

    VideoStats.refresh();
};

VideoStats.refresh = function() {
    // TODO(benkomalo): consolidate this with the server info in
    // daily-ex-stats.js (maybe abstract to a data fetcher)
    var BASE_STAT_SERVER_URL = "http://184.73.72.110:27080/";

    var url = BASE_STAT_SERVER_URL + "report/daily_video_stats/_find?callback=?";
    var datestamp = $("#datestamp").val();
    var user_category = $("#user_category").val();
    var criteria = '{"date_str":"' + datestamp + '","ucat":"'
        + user_category+ '"}';
    var params = {
        //json query
        "criteria": criteria,
        "batch_size": 15000
    };
    $.getJSON(url, params, VideoStats.handleDataLoad);

    $("#video-stats-container").html("Loading...");
};


/**
 * Handles the raw JSON data returned from the server.
 * @param {Object} data The raw data with fields including:
 *     rows - JSON object for each video record summary
 *     total_rows - total length of the rows
 *     query - JSON object representing the original query
 */
VideoStats.handleDataLoad = function(data) {
    var results = data["results"];
    for (var i = 0; i < results.length; i+=1) {
        var row = results[i];
        if (row["vid"] === "total") {
            row["link"] = "<b>Total</b>"
        } else {
            row["link"] = '<a href="http://youtube.com/watch?v=' +
                row["vid"] + '">' + row["vtitle"] + '</a>';
        }
        row["hours_watched"] = Math.floor(row["seconds_watched"] / 3600);
        row["percent_completed"] = (row["completed"] / row["watched"]) || 0;
    }

    VideoStats.renderVideosTable(results);
};

// TODO(benkomalo): have a configurable sort
/**
 * Renders the main stats table. Each row is a record summarizing the stats on
 * a video level (e.g. how many people completed it).
 */
VideoStats.renderVideosTable = function(jsonRows) {
    var container = $("#video-stats-container");
    if (!(jsonRows && jsonRows.length)) {
        container.html("<strong>No data for that date :(</strong>");
        return;
    }
    var tableTemplate = Handlebars.compile($("#video-table").text());
    var table = $(tableTemplate());

    var rowTemplate = Handlebars.compile($("#video-row-template").text());
    _.chain(jsonRows)
        .sortBy(function(row) { return -row["seconds_watched"]; })
        .each(function(row) { $(rowTemplate(row)).appendTo(table) });

    container.html("");
    container.append(table);
};

$(document).ready(function() {
    VideoStats.init();
});

