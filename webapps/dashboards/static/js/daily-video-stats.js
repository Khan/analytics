/**
 * Logic for fetching data and rendering the daily video
 * statistics dashboard.
 */

// Namespace
var VideoStats = {};

/**
 * Raw JSON cache of data keyed off of URLs
 */
VideoStats.cache_ = {};

/**
 * A wrapper over jQuery.getJSON, which caches the results.
 */
VideoStats.getJson = function(url, params, callback) {
    var cacheKey = url;
    if (params) {
        cacheKey += JSON.stringify(params);
    }
    if (_.has(VideoStats.cache_, cacheKey)) {
        // Asynchronously call the callback to match getJSON behaviour.
        var deferred = $.Deferred();
        _.defer(function() {
            callback(VideoStats.cache_[cacheKey]);
            deferred.resolve();
        });
        return deferred;
    }

    var callbackProxy = function(data) {
        VideoStats.cache_[cacheKey] = data;
        callback(data);
    };
    return $.getJSON(url, params, callbackProxy);
};

/**
 * Entry point - called on DOMReady event.
 */
VideoStats.init = function() {
    // Note that JS Date implementation does "the right thing" even if
    // today is the first of the month.
    var yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);

    $("#daily-video-date")
        .datepicker({ dateFormat: "yy-mm-dd" })
        .datepicker("setDate", yesterday);
    $("#daily-video-date").change(VideoStats.refresh);
    $("#user-category").change(VideoStats.refresh);

    VideoStats.refresh();
};

VideoStats.refresh = function() {
    // TODO(benkomalo): consolidate this with the server info in
    // daily-ex-stats.js (maybe abstract to a data fetcher)
    var BASE_STAT_SERVER_URL = "http://184.73.72.110:27080/";

    var url = BASE_STAT_SERVER_URL +
            "report/daily_video_stats/_find?callback=?";
    var datestamp = $("#daily-video-date").val();
    var userCategory = $("#user-category").val();
    var criteria = JSON.stringify({
        "date_str": datestamp,
        "ucat": userCategory
    });
    var sort = JSON.stringify({
        "-seconds_watched": 1
    });
    var params = {
        // JSON query
        "criteria": criteria,
        "batch_size": 15000,
        "sort": sort
    };

    $("#individual-video-summary-container").text("Loading...");
    VideoStats.getJson(url, params, VideoStats.handleDataLoadForDay);
};

/**
 * Handles the raw JSON data returned from the server for the
 * data about individual video summaries for a given date.
 * @param {Object} data The raw data with fields including:
 *     rows - JSON object for each video record summary
 *     total_rows - total length of the rows
 *     query - JSON object representing the original query
 */
VideoStats.handleDataLoadForDay = function(data) {
    var results = data["results"];
    for (var i = 0; i < results.length; i += 1) {
        var row = results[i];
        if (row["vid"] === "total") {
            row["link"] = "<b>Total Across All Videos</b>";
        } else {
            row["link"] = '<a href="http://youtube.com/watch?v=' +
                row["vid"] + '">' + row["vtitle"] + "</a>";
        }
        row["hours_watched"] = Math.floor(row["seconds_watched"] / 3600);
        row["percent_completed"] = (row["completed"] / row["watched"]) || 0;
    }

    VideoStats.renderVideoSummary(results);
};

// TODO(benkomalo): have a configurable sort
/**
 * Renders the table summarizing activity by individual videos.
 * Each row is a record summarizing the stats on a video level
 * (e.g. how many people completed it).
 */
VideoStats.renderVideoSummary = function(jsonRows) {
    var container = $("#individual-video-summary-container");
    if (!(jsonRows && jsonRows.length)) {
        container.html("<strong>No data for that date :(</strong>");
        return;
    }
    var tableTemplate = Handlebars.compile($("#video-table").text());
    var table = $(tableTemplate());

    var rowTemplate = Handlebars.compile($("#video-row-template").text());
    _.chain(jsonRows)
        .sortBy(function(row) { return -row["seconds_watched"]; })
        .first(20)
        .each(function(row) { $(rowTemplate(row)).appendTo(table) });

    container.html("");
    container.append(table);
};

$(document).ready(function() {
    VideoStats.init();
});

