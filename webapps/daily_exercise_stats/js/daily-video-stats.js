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
    // TODO(benkomalo): consolidate this with the server info in
    // daily-ex-stats.js (maybe abstract to a data fetcher)
    var BASE_STAT_SERVER_URL = "http://184.73.72.110:28017/";

    var url = BASE_STAT_SERVER_URL + "report/daily_video_stats/?jsonp=?";
    var params = {
        // user-category filter
        "filter_ucat": "all",
        "data_date_str": "2012-05-22"  // TODO(benkomalo): abstract away!
    };
    $.getJSON(url, params, VideoStats.handleDataLoad);
};

/**
 * Handles the raw JSON data returned from the server.
 * @param {Object} data The raw data with fields including:
 *     rows - JSON object for each video record summary
 *     total_rows - total length of the rows
 *     query - JSON object representing the original query
 */
VideoStats.handleDataLoad = function(data) {
    if (!data["rows"]) {
        // TODO(benkomalo): handle - data is empty.
    }

    VideoStats.renderVideosTable(data["rows"]);
};

// TODO(benkomalo): have a configurable sort
/**
 * Renders the main stats table. Each row is a record summarizing the stats on
 * a video level (e.g. how many people completed it).
 */
VideoStats.renderVideosTable = function(jsonRows) {
    var tableTemplate = Handlebars.compile($("#video-table").text());
    var table = $(tableTemplate());

    var rowTemplate = Handlebars.compile($("#video-row-template").text());
    _.chain(jsonRows)
        .sortBy(function(row) { return -row["seconds_watched"]; })
        .each(function(row) { $(rowTemplate(row)).appendTo(table) });

    var container = $("#video-stats-container");
    container.append(table);
};
