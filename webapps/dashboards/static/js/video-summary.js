/**
 * Logic for fetching data and rendering the daily video
 * statistics dashboard.
 */

(function() {

/**
 * Entry point - called on DOMReady event.
 */
var init = function() {

    $("#video-start-date")
        .datepicker({ dateFormat: "yy-mm-dd"})
        .change(refreshVideoSummary);
    $("#video-end-date")
        .datepicker({ dateFormat: "yy-mm-dd"})
        .change(refreshVideoSummary);
    $("#video-summary-timescale")
        .change(refreshVideoSummary);
    
    var timeScale = $("#video-summary-timescale").val();
    charts["users"] = initChart("video-summary-users-graph-container",
                                "Users", timeScale);
    charts["visits"] = initChart("video-summary-visits-graph-container",
                                "Visits", timeScale);
    charts["hours"] = initChart("video-summary-hours-graph-container",
                                "Hours Watched", timeScale);

    refreshVideoSummary();
    initAutoComplete();
};


var charts = {};
/**
 * This function converts a string such as '2012-08-21' and returns
 * the corresponding Date object.
 */
var strToDate = function(strDate) {
    dateParts = strDate.split("-");
    return Date.UTC(dateParts[0], (dateParts[1] - 1), dateParts[2]);
};

/**
 * initChart: Builds a HighChart object for the given video series.
 */
var initChart = function(containerName, dataType, duration) {
    
    var chartTitle = makeChartTitle(dataType, duration);
    return new Highcharts.Chart({
            chart: {
                renderTo: containerName,
                type: 'spline'
            },
            title: {
                text: chartTitle
            },
            xAxis: {
                type: "datetime",
                dateTimeLabelFormats: { day: "%a %e %b" }
            },
            yAxis: {
                title: {
                    text: dataType
                },
                labels: {
                    formatter: function() {
                        return this.value; 
                    }
                }
            },
            tooltip: {
                crosshairs: true,
                shared: true
            },
            plotOptions: {
                spline: {
                    marker: {
                        radius: 4,
                        lineColor: '#666666',
                        lineWidth: 1
                    }
                }
            },
            series: []
        });
};

/**
 * Render all the graphics on the page
 */
var renderVideoSummaryGraphs = function(results) {
    
    var userSeries = createGraphSeries('watched', results);
    reloadChart(userSeries, charts['users']);
    var visitsSeries = createGraphSeries('visits', results);
    reloadChart(visitsSeries, charts['visits']);
    var hoursSeries = createGraphSeries('hours', results);
    reloadChart(hoursSeries, charts['hours']);

};

/**
 * Reload chart with new series data
 */
var reloadChart = function(series, chart) {
    for (var s; s = chart.series[0]; ) {
        s.remove(true);
    }
    chart.showLoading();  
    chart.addSeries({"name": "All", "data": series['all']}, false);
    chart.addSeries({"name": "Registered", "data": series['registered']}, 
                    false);
    chart.addSeries({"name": "Phantom", "data": series['phantom']}, false);
    chart.hideLoading();
    chart.redraw();

};

/**
 * Generate data series for graphics based on results
 */
var createGraphSeries = function(prefix, results) {
    var series = {'all' : [],
                  'registered' : [],
                  'phantom' : []};
    _.chain(results)
     .sortBy(function(row) { return row["dt"]; })
     .each(function(record) {
        dt = strToDate(record["dt"]);
        series['all'].push([dt, record[prefix + '_all']]);                
        series['registered'].push([dt, record[prefix + '_registered']]);
        series['phantom'].push([dt, record[prefix + '_phantom']]);
    })
    return series;
};

/**
 * Initialize the autocomplete module
 */
var initAutoComplete = function() {
    var url = "/db/distinct_video_titles"
    AjaxCache.getJson(url, {}, populateAutocompleteList);
};

/**
 * Callback for populating the autocomplete data
 */
var populateAutocompleteList = function(data) {
    var video_titles = data['video_titles'];    
    $("#video-title-typeahead").autocomplete({
        source: video_titles,
        select: function(event, ui) {
                    $("#video-title-selected").val(ui.item.value);
                    refreshVideoSummary(); 
                }
        }
    )
};

/**
 * Refresh the video summary table and charts
 */
var refreshVideoSummary = function() {
    var url = "/data/video_title_summary?";
    var start_date = $("#video-start-date").val();
    var end_date = $("#video-end-date").val();
    var time_scale = $("#video-summary-timescale").val();
    var title = $("#video-title-selected").val();
    var params = {
        "start_date": start_date,
        "end_date": end_date,
        "time_scale": time_scale,
        "title" : title
    };
    var display_title = 'Video Statistics for "' + title + '"';
    if (title.length === 0 || title === 'Total') {
        display_title = 'Video Statistics for all videos';
    }
    $("#video-summary-title").html(display_title);
    $("#video-summary-table-container").text("Loading...");
    refreshChartTitles(time_scale);
    AjaxCache.getJson(url, params, handleDataLoadForVideoSummary);
};

var refreshChartTitles = function(timeScale) {
    for (key in charts) {
        var title = makeChartTitle(key, timeScale);
        charts[key].setTitle({"text": title});
    }
};

var makeChartTitle = function(chartType, timeScale) {
    var titleMap = {'users': 'Users', 
        'visits': 'Visits', 
        'hours': 'Hours Watched'};
    return titleMap[chartType] + ' by ' + timeScale;    
};

/**
 * Handles the raw JSON data returned from the server for the
 * data about video time series data
 */
var handleDataLoadForVideoSummary = function(data) {
    var results = data["results"];
    renderVideoSummary(results, $("#video-summary-table-container"));
    renderVideoSummaryGraphs(results);
};


// TODO(benkomalo): have a configurable sort
/**
 * Renders the video summary table
 * Each row is a record summarizing the stats for a time duration
 * container is the div where we will show the results
 */
var renderVideoSummary = function(jsonRows, container) {
    if (!(jsonRows && jsonRows.length)) {
        container.html("<strong>No data for that date :(</strong>");
        return;
    }
    var tableTemplate = Handlebars.compile($("#video-summary-table").text());
    var table = $(tableTemplate());
    var rowTemplate = Handlebars.compile($("#video-summary-row-template").text());
    _.chain(jsonRows)
        .sortBy(function(row) { return row["dt"]; })
        .each(function(row) { $(rowTemplate(row)).appendTo(table) });

    container.html("");
    container.append(table);
};

$(document).ready(function() {
    init();
});

})();
