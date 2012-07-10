/**
 * Logic for fetching data and rendering the daily video
 * statistics dashboard.
 */

(function() {

var BASE_DB_URL = "http://107.21.23.204:27080/report/";

/**
 * A HighCharts object that shows the video usage over time on a stacked graph
 * that segments users by engagement levels.
 * @type {HighCharts.Chart}
 */
var chart = null;

/**
 * The saved series data, keyed by a date range string.
 * Each entry in a date range is another Object, keyed by the
 * number of days that user segmented watched a video in the specified
 * date range. There will also always be one additional entry for the
 * "total" for that date range.
 * {
 *   dateRangeStr: {
 *     0: {
 *       num_users: ...,
 *       mins_per_user: ...,
 *       completed_per_user: ...,
 *       ...
 *     },
 *     1: {
 *       ...
 *     },
 *     ...
 *     total: {
 *        ...
 *     }
 *   },
 *   dateRangeStr: {
 *      ...
 *   },
 *   ...
 * }
 */
var engagementData_ = {};


/**
 * Entry point - called on DOMReady event.
 */
var init = function() {
    // Note that JS Date implementation does "the right thing" even if
    // today is the first of the month.
    var yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);

    $("#daily-video-date")
        .datepicker({ dateFormat: "yy-mm-dd" })
        .datepicker("setDate", yesterday);
    $("#daily-video-date").change(refreshDailyActivity);
    $("#user-category").change(refreshDailyActivity);
    refreshDailyActivity();

    chart = createUsageGraph();

    $("#engagement-summary-type").change(refreshEngagementSummary);
    refreshEngagementSummary();
};

/**
 * Generates an array of objects representing date ranges.
 * @param {string} type Either "week" or "month".
 * @param {Date} referenceDate The date to start generating ranges
 *     backwards from
 * @param {number} nRanges The number of date ranges to generate.
 */
var generateDateRanges = function(type, referenceDate, nRanges) {
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

/**
 * Fetches and refreshes data from the server about the last N ranges.
 */
var fetchEngagementData = function(ranges, type) {
    var url = BASE_DB_URL + "user_video_distribution/_find?callback=?";

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
        // the caching feature of AjaxCache.getJson, since we need to
        // manually cache each range result.
        deferreds.push($.getJSON(
            url, params, function(data) {
                handleEngagementDataLoad(range, data["results"] || []);
            }));
    });

    $("#engagement-summary-table-container").text("Loading...");
    $.when.apply($, deferreds).done(function() {
        // All ranges finished loading - render.
        renderUsageGraph(ranges, type);

        // TODO(benkomalo): do something more useful instead of
        // rendering a table for each time period. Maybe on-hover
        // or on-click of the graph, show the detailed table?
        renderEngagementTables(ranges, type);
    });
};

/**
 * Handles a raw data load from the server and massages/saves it.
 * @param {string} range The date range string that the data is for>
 * @param {Array.<Object>} data A list of raw records from the server.
 */
var handleEngagementDataLoad = function(range, data) {
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
    engagementData_[JSON.stringify(range)] = dataByVisit;
};

/**
 * Builds a stacked graph of video usage over time, segmented by user
 * engagement levels.
 */
var createUsageGraph = function() {
    var chartOptions = {
        chart: {
            renderTo: "engagement-summary-graph-container",
            type: "area"
        },
        title: {
            text: "Video Usage Over Time"
        },
        plotOptions: {
			area: {
				stacking: "normal",
				lineColor: "#666666",
				lineWidth: 1,
				marker: {
					lineWidth: 1,
					lineColor: "#666666"
				}
			}
		},
        series: [],
        yAxis: {
            title: { text: "Num users" }
        },
        xAxis: {
            title: { text: "Week of" },
            min: 0,
            max: 10
        },
        credits: { enabled: false }
    };

    var chart = new Highcharts.Chart(chartOptions);
    return chart;
};

/**
 * Kicks off a data fetch for new data according to the specified params.
 */
var refreshEngagementSummary = function() {
    var n = 10;
    var today = new Date();
    var type = $("#engagement-summary-type").val();
    var ranges = generateDateRanges(type, today, n);

    // Remove all existing series in the chart.
    for (var series; series = chart.series[0]; ) {
        series.remove(true);
    }

    engagementData_ = {};
    chart.showLoading();
    fetchEngagementData(ranges, type);
};

/**
 * Renders the graph of video usage over time.
 */
var renderUsageGraph = function(ranges, type) {
    var max = type === "week" ? 7 : 31;
    var dataKeys = _.map(ranges, function(r) { return JSON.stringify(r); });

    var buildSeries = function(max, name) {
        return {
            max: max,
            series: {
                name: name,
                data: []
            }
        };
    };

    var seriesByBucket;
    if (type === "week") {
        seriesByBucket = [
            buildSeries(1, "Transient (1 day)"),
            buildSeries(3, "Regular (2-3 days)"),
            buildSeries(7, "Heavy (4+ days)")
        ];
    } else {
        seriesByBucket = [
            buildSeries(1, "Transient (1 day)"),
            buildSeries(4, "Light (2-4 days)"),
            buildSeries(7, "Regular (5-7 days)"),
            buildSeries(31, "Heavy (8+ days)")
        ];
    }

    _.each(dataKeys, function(dataKey) {
        var data = engagementData_[dataKey] || [];
        var curValue = 0;
        var curBucketIx = 0;
        var curBucket = seriesByBucket[0];
        for (var i = 1; i <= max; i++) {
            if (i > curBucket.max) {
                curBucket.series.data.push(curValue);
                curValue = 0;
                curBucket = seriesByBucket[++curBucketIx];
            }
            var series = []
            var dayBucket = data[i] || {};
            var numUsers = dayBucket["num_users"] || 0;
            curValue += numUsers;
        }
        curBucket.series.data.push(curValue);
    });

    _.each(seriesByBucket, function(bucket) {
        chart.addSeries(bucket.series, /* redraw */ false);
    });
    chart.hideLoading();
    chart.redraw();
};

/**
 * Renders the tables of video engagement data.
 * This renders a set of columns for each date range. Inside each date range
 * is a set of columns for data about video usage per user.
 * The rows bucket the users into "days engaged on the site", which basically
 * means the number of days we detected they watched any videos in the date
 * range.
 */
var renderEngagementTables = function(ranges, type) {
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
        var data = engagementData_[dataKey] || [];
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
            var data = engagementData_[dataKey] || [];
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






// TODO(benkomalo): perhaps the "daily activity" tables should go to a
// different dashboard that highlights info re: content usage?

var refreshDailyActivity = function() {
    // TODO(benkomalo): consolidate this with the server info in
    // daily-ex-stats.js (maybe abstract to a data fetcher)

    var url = BASE_DB_URL + "daily_video_stats/_find?callback=?";
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
    AjaxCache.getJson(url, params, handleDataLoadForDay);
};

/**
 * Handles the raw JSON data returned from the server for the
 * data about individual video summaries for a given date.
 * @param {Object} data The raw data with fields including:
 *     rows - JSON object for each video record summary
 *     total_rows - total length of the rows
 *     query - JSON object representing the original query
 */
var handleDataLoadForDay = function(data) {
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

    renderVideoSummary(results);
};

// TODO(benkomalo): have a configurable sort
/**
 * Renders the table summarizing activity by individual videos.
 * Each row is a record summarizing the stats on a video level
 * (e.g. how many people completed it).
 */
var renderVideoSummary = function(jsonRows) {
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
    init();
});


})();
