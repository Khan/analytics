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

    $("#daily-video-date")
        .datepicker({ dateFormat: "yy-mm-dd" })
        .datepicker("setDate", yesterday);
    $("#daily-video-date").change(VideoStats.refreshDailyActivity);
    $("#user-category").change(VideoStats.refreshDailyActivity);
    VideoStats.refreshDailyActivity();

    $("#engagement-summary-type").change(VideoStats.refreshEngagementSummary);
    VideoStats.refreshEngagementSummary();
};

/**
 * Generates an array of objects representing date ranges.
 * @param {string} type Either "week" or "month".
 * @param {Date} referenceDate The date to start generating ranges
 *     backwards from
 * @param {number} nRanges The number of date ranges to generate.
 */
VideoStats.generateDateRanges = function(type, referenceDate, nRanges) {
    var ranges = [];
    if (type === "week") {
        // Find the first Sunday of this week.
        var offset = referenceDate.getDay();  // 0-based Day of Week from Sunday
        var sunday = new Date(referenceDate);
        sunday.setDate(referenceDate.getDate() - offset);

        for (var i = nRanges; i >= 1; i--) {
            var rangeStart = new Date(sunday);
            var rangeEnd = new Date(sunday);
            rangeStart.setDate(sunday.getDate() - 7 * i);
            rangeEnd.setDate(sunday.getDate() - 7 * (i - 1));
            ranges.push({
                "start": rangeStart,
                "end": rangeEnd
            });
        }
    } else {
        // Month
        var first = new Date(referenceDate);
        first.setDate(1);

        for (var i = nRanges - 1; i >= 0; i--) {
            var rangeStart = new Date(first);
            var rangeEnd = new Date(first);
            rangeStart.setMonth(first.getMonth() - i);
            rangeEnd.setMonth(first.getMonth() - (i - 1));
            ranges.push({
                "start": rangeStart,
                "end": rangeEnd
            });
        };
    }
    return ranges;
};

VideoStats.refreshEngagementSummary = function() {
    var type = $("#engagement-summary-type").val();
    var nRanges = 3;

    // TODO(benkomalo): hacking in a reference date until a backfill is done.
    // Get the last N ranges of the type
    //var today = new Date();
    var today = new Date(2012, 3, 22);
    var ranges = VideoStats.generateDateRanges(type, today, nRanges);

    var BASE_STAT_SERVER_URL = "http://184.73.72.110:27080/";
    var url = BASE_STAT_SERVER_URL + "report/user_video_distribution/_find?callback=?";
    var deferreds = [];
    // TODO(benkomalo): deal with phantom users!
    _.each(ranges, function(range) {
        var criteria = JSON.stringify({
            "start_dt": $.datepicker.formatDate("yy-mm-dd", range["start"]),
            "end_dt": $.datepicker.formatDate("yy-mm-dd", range["end"]),
            "registered": true
        });
        var params = {
            "criteria": criteria,
            "batch_size": 15000
        };
        // Asynchronously load each range. Note we don't need to use
        // the caching feature of VideoStats.getJson, since we need to
        // manually cache each range result.
        deferreds.push($.getJSON(
            url, params, function(data) {
                VideoStats.handleEngagementDataLoad(
                    range,
                    data["results"] || []);
            }));
    });

    $("#engagement-summary-table-container").text("Loading...");
    $.when.apply($, deferreds).done(function() {
        // All ranges finished loading - render.
        VideoStats.renderEnagementTables(ranges, type);
    });
};

VideoStats.engagementData_ = {};

/**
 * Handles a data load for a date range of video engagement data,
 * transforming it and caching it for later rendering.
 */
VideoStats.handleEngagementDataLoad = function(range, data) {
    // Fields we care about for now.
    var fields = [
        "num_users", // Unique users
        "completed", // Completed videos
        "seconds"    // Seconds watched
    ];

    // Bucket the users by days they visited the site and watched a video.
    var dataByVisit = {};
    _.each(data, function(record) {
        var visits = record["visits"];
        var modifiedRecord =  _.pick.apply(_, [record].concat(fields));
        var numUsers = record["num_users"] || 0;
        modifiedRecord["completed_per_user"] =
                (record["completed"] || 0) / numUsers;
        modifiedRecord["mins_per_user"] =
                (record["seconds"] || 0) / 60.0 / numUsers;
        dataByVisit[visits] = modifiedRecord;
    });

    var totals = {};
    _.each(fields, function(field) {
        // Sum across buckets.
        // A bucket is data about users who were engaged for X number
        // of days (i.e. came back and watched a video across X days).
        totals[field] = _.reduce(data, function(sum, bucket) {
            return sum + bucket[field];
        }, 0);
    });

    var numUsers = totals["num_users"] || 0;
    totals["completed_per_user"] = (totals["completed"] || 0) / numUsers;
    totals["mins_per_user"] = (totals["seconds"] || 0) / 60.0 / numUsers;
    dataByVisit["total"] = totals;
    VideoStats.engagementData_[JSON.stringify(range)] = dataByVisit;
};

/**
 * Renders the tables of video engagement data.
 * This renders a set of columns for each date range. Inside each date range
 * is a set of columns for data about video usage per user.
 * The rows bucket the users into "days engaged on the site", which basically
 * means the number of days we detected they watched any videos in the date
 * range.
 */
VideoStats.renderEnagementTables = function(ranges, type) {
    // Render the first header showing date ranges.
    var tableHtml = [
        "<table class=\"engagement-summary table ",
            "table-bordered table-striped\">",
        "<thead><th></th>" // Leading blank cell.
    ];
    _.each(ranges, function(range) {
        var start = range["start"];
        var end = range["end"];
        var headerStr;
        if (type === "week") {
            headerStr = "Sun " + $.datepicker.formatDate("mm/dd", start) +
                " - " +
                "Sun " + $.datepicker.formatDate("mm/dd", end);
        } else {
            // Month
            headerStr = $.datepicker.formatDate("M", start);
        }
        tableHtml.push("<th colspan=3>" + headerStr + "</th>");
    });

    // Render the second header of column headers. Each date range consists
    // of multiple columns.
    tableHtml.push("</thead><tr><td>Days engaged</td>");
    _.times(ranges.length, function() {
        tableHtml.push("<td width='80px'># Users</td>",
            "<td>Vids completed / user</td>",
            "<td>Mins / user</td>");
    });
    tableHtml.push("</tr>");

    // Pre-compute the data keys for each date range.
    var dataKeys = _.map(ranges, function(r) { return JSON.stringify(r); });

    // Render a totals in a first row.
    tableHtml.push("<tr><td><b>Totals</b></td>");
    _.each(dataKeys, function(dataKey) {
        var data = VideoStats.engagementData_[dataKey] || [];
        var totals = data["total"] || {};
        var numUsers = totals["num_users"] || 0;
        var vidsPerUser = (totals["completed_per_user"] || 0).toFixed(2);
        var minsPerUser = (totals["mins_per_user"] || 0).toFixed(2);
        tableHtml.push(
            "<td><b>", numUsers, "</b></td>",
            "<td><b>", vidsPerUser, "</b></td>",
            "<td><b>", minsPerUser, "</b></td>"
        );
    });
    tableHtml.push("</tr>");

    // Render each row of possible days engaged (7 or 31).
    var max = type === "week" ? 7 : 31;
    for (var i = 1; i <= max; i++) {
        tableHtml.push("<tr><td>" + i + "</td>");
        _.each(dataKeys, function(dataKey) {
            var data = VideoStats.engagementData_[dataKey] || [];
            var totals = data["total"] || {};
            data = data[i] || {};
            var numUsers = data["num_users"] || 0;
            var numUsersP = Math.round(
                numUsers * 100.0 / totals["num_users"]) || 0;
            var vidsPerUser = (data["completed_per_user"] || 0).toFixed(2);
            var minsPerUser = (data["mins_per_user"] || 0).toFixed(2);
            tableHtml.push(
                "<td>", numUsers, " (", numUsersP, "%)</td>",
                "<td>", vidsPerUser, "</td>",
                "<td>", minsPerUser, "</td>"
            );
        });
        tableHtml.push("</tr>");
    }
    tableHtml.push("</table>");

    $("#engagement-summary-table-container").html(tableHtml.join(""));
};

VideoStats.refreshDailyActivity = function() {
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
    AjaxCache.getJson(url, params, VideoStats.handleDataLoadForDay);
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

