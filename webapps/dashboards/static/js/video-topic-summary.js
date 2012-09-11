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

    $("#topic-summary-date")
        .datepicker({ dateFormat: "yy-mm-dd" })
        .datepicker("setDate", lastMonth);
    $("#topic-summary-date").change(refreshTopicSummary);
    refreshTopicSummary();
};

/**
 * Refresh the topic tables
 */
var refreshTopicSummary = function() {
    var url = "/data/topic_summary?";
    var start_date = $("#topic-summary-date").val();
    var time_scale = $("#topic-summary-timescale").val();
    var params = {
        "start_date": start_date,
        "time_scale": time_scale,
    };

    $("#video-top-topic-table-container").text("Loading...");
    $("#video-second-topic-table-container").text("Loading...");
    AjaxCache.getJson(url, params, handleDataLoadForTopicSummary);
};

/**
 * Handles the raw JSON data returned from the server for the
 * data about top topics and secondary topics
 */
var handleDataLoadForTopicSummary = function(data) {
    var top_results = data["top_results"];
    var second_results = data["second_results"];

    renderTopicSummary(top_results, $("#video-top-topic-table-container"));
    renderTopicSummary(second_results, 
                       $("#video-second-topic-table-container"));
};

// TODO(benkomalo): have a configurable sort
/**
 * Renders the topic summary table
 * Each row is a record summarizing the stats for a topic
 * container is the div where we will show the results
 * (e.g. how many people completed it).
 */
var renderTopicSummary = function(jsonRows, container) {
    if (!(jsonRows && jsonRows.length)) {
        container.html("<strong>No data for that date :(</strong>");
        return;
    }
    var tableTemplate = Handlebars.compile($("#topic-table").text());
    var table = $(tableTemplate());
    var rowTemplate = Handlebars.compile($("#topic-row-template").text());
    _.chain(jsonRows)
        .sortBy(function(row) { return -row["hours_all"]; })
        .each(function(row) {
            row["href"] = "/video-summary?title=" + encodeURIComponent(row["title"]);
            $(rowTemplate(row)).appendTo(table);
         });

    container.html("");
    container.append(table);
};

$(document).ready(function() {
    init();
});


})();
