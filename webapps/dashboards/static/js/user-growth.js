/**
 * Logic for fetching data and rendering the user growth dashboard.  Currently,
 * the page plots account state transitions (joins, deactivations,
 * reactivations) versus time.
 */

(function() {

// TODO(jace): centralize this to a analytics.js file?
var BASE_DB_URL = "http://107.21.23.204:27080/report/";

/**
 * A HighCharts object that shows the number of account transitions of various
 * types versus time.
 * @type {HighCharts.Chart}
 */
var chart = null;


/**
 * Holds the data for the time series in a format accepted by Highcharts.
 * There should be one series for each key of seriesSigns (below), plus
 * one NET series which adds up the signed sum of all other series.
 */
var growthSeries_ = {};

/**
 * Holds the grand totals keyed by series (including NET) over the date range.
 */
var growthTotals_ = {};

/**
 * These are the series names expected to be in the database and their
 * associated sign.  E.g., a joins represents the gain (positive) of a user,
 * while a reactivations represents a loss (negative) of a user.
 */
var seriesSigns = {
    "joins": 1,
    "deactivations": -1,
    "reactivations": 1
};
var seriesTypes = _.keys(seriesSigns);


/**
 * This function converts a string such as '2012-08-21' and returns
 * the corresponding Date object.
 */
var strToDate = function(strDate) {
    dateParts = strDate.split("-");
    return Date.UTC(dateParts[0], (dateParts[1] - 1), dateParts[2]);
};


/**
 * Create a comma-fied string representation of a number.
 */
var numberWithCommas = function(x) {
    return x.toString().replace(/\B(?=(?:\d{3})+(?!\d))/g, ",");
};

/**
 * Entry point - called on DOMReady event.
 */
var init = function() {
    chart = createGrowthGraph();

    var defaultStartDate = new Date(2011, 0, 1);
    $("#growth-summary-startdate")
        .datepicker({ dateFormat: "yy-mm-dd" })
        .datepicker("setDate", defaultStartDate);

    var yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    $("#growth-summary-enddate")
        .datepicker({ dateFormat: "yy-mm-dd" })
        .datepicker("setDate", yesterday);

    $("#growth-summary-startdate").change(refreshGrowthSummary);
    $("#growth-summary-enddate").change(refreshGrowthSummary);
    $("#growth-summary-timescale").change(refreshGrowthSummary);

    refreshGrowthSummary();

    $("#def-and-faq-header").click(function() {
        $("#def-and-faq-body").toggle();
    });
    $("#def-and-faq-body").toggle(); // initialize as hidden
};


/**
 * Initialize the growth graph with it formatting options.
 */
var createGrowthGraph = function() {
    var chartOptions = {
        chart: {
            renderTo: "growth-summary-graph-container",
            defaultSeriesType: "spline"
        },
        title: {
            text: "Account state transitions over time"
        },
        xAxis: {
            type: "datetime",
            dateTimeLabelFormats: { day: "%a %e %b" }
        },
        yAxis: {
            title: { text: "" }
        },
        series: [],
        credits: { enabled: false }
    };

    var chart = new Highcharts.Chart(chartOptions);
    return chart;
};


/**
 * Makes an AJAX call for each of the data series, and registers callback
 * functions to process the responses.
 */
var fetchGrowthData = function(postFetchCallback) {
    var url = BASE_DB_URL + "user_growth/_find?callback=?";

    var deferreds = [];

    _.each(seriesTypes, function(series) {

        var criteria = JSON.stringify({
            "timescale": $("#growth-summary-timescale").val(),
            "series": series,
            "dt": {
                "$gte": $("#growth-summary-startdate").val(),
                "$lt": $("#growth-summary-enddate").val()
            }
        });
        var params = {
            "criteria": criteria,
            "batch_size": 15000,
            "sort": JSON.stringify({"dt": 1})
        };
        deferreds.push($.getJSON(
            url, params, function(data) {
                handleGrowthSeries(series, data["results"] || []);
            }));
    });

    $.when.apply($, deferreds).done(postFetchCallback);
};


/**
 * Process the response for a query of the mongo REST API.  The response
 * represents a single time series.  We convert to the format expected by
 * Highcharts an store in in the main data structure, keyd by series name.
 */
var handleGrowthSeries = function(series, data) {

    var rows = [];
    _.each(data, function(record) {
        rows.push([strToDate(record["dt"]), record["value"]]);
    });

    growthSeries_.push({"name": series, "data": rows});
};

/**
 * Computes the net gain or loss of active accounts, and inserts the result
 * as a new time series in the main data structure.
 */
var netGrowthSeries = function() {

    var rows = [];

    var maxLength = 0;
    _.each(growthSeries_, function(series) {
        maxLength = Math.max(maxLength, series.data.length);
    });

    for (var d = 0; d < maxLength; d++) {
        var netDelta = 0;
        var netDate = null;
        for (var s = 0; s < growthSeries_.length; s++) {
            if (d + growthSeries_[s].data.length >= maxLength) {
                // The growth series are not necessarily the same length
                // because, e.g., deactivations can only start occuring 28 days
                // aftera join. The series should all "align" on the ending
                // date interval, not the beginning interval.  The offset
                // below is a non-positive number that keeps the index
                // for shorter series aligned and within array bounds.
                var offset = growthSeries_[s].data.length - maxLength;
                var delta = growthSeries_[s].data[d + offset][1];
                delta = delta * seriesSigns[growthSeries_[s].name];
                netDelta += delta;

                netDate = growthSeries_[s].data[d + offset][0];
            }
        }
        rows.push([netDate, netDelta]);
    }

    growthSeries_.push({"name": "NET", "data": rows});
};


/**
 * Compute the total change for each series over the entire date range.
 */
var totalGrowth = function() {

    _.each(growthSeries_, function(series) {
        var seriesTotal = 0;
        _.each(series.data, function(row) {
            seriesTotal += row[1];
        });
        growthTotals_[series.name] = seriesTotal;
    });

};


/**
 * Retrieve the data requested from the UI, and refresh the chart with the
 * new data.
 */
var refreshGrowthSummary = function() {
    // Remove all existing series in the chart.
    for (var series; series = chart.series[0]; ) {
        series.remove(true);
    }

    growthSeries_ = [];
    chart.showLoading();

    fetchGrowthData(function() {

        netGrowthSeries();

        _.each(growthSeries_, function(series) {
            chart.addSeries(series, /* redraw */ false);
        });

        chart.hideLoading();
        chart.redraw();

        totalGrowth();

        _.each(growthTotals_, function(total, seriesName) {
            $("#growth-table-" + seriesName).text(numberWithCommas(total));
        });

    });
};


$(document).ready(function() {
    init();
});


})();
