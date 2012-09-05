/**
 * Dashboard page based on Q3 2012 company growth goals.
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
 * This function converts a string such as '2012-08' and returns
 * a corresponding Date object for the first day of that month.
 */
var monthStringToDate = function(strDate) {
    dateParts = strDate.split("-");
    return Date.UTC(dateParts[0], (dateParts[1] - 1), 1);
};

/**
 * Initialize the graph with its formatting options.
 */
var createGoalGraph = function(dataSeries) {
    var chartOptions = {
        chart: {
            renderTo: "goal-summary-graph-container",
            defaultSeriesType: "spline"
        },
        title: {
            text: "Monthly User Counts for Company Growth Metrics"
        },
        xAxis: {
            type: "datetime",
            dateTimeLabelFormats: { day: "%a %e %b" }
        },
        yAxis: {
            title: { text: "" }
        },
        series: dataSeries,
        credits: { enabled: false }
    };

    var chart = new Highcharts.Chart(chartOptions);
    return chart;
};


/**
 * Makes an AJAX call for each of the data series, and registers a callback
 * function to process the responses.
 */
var fetchGrowthData = function() {
    var url = BASE_DB_URL + "company_metrics/_find?callback=?";

    var params = {
        "batch_size": 15000,
        "sort": JSON.stringify({"month": 1})
    };
    $.getJSON(url, params, function(data) {
        handleGrowthData(data["results"] || []);
    });
};


/**
 * Process the response for a query of the mongo REST API.  The response
 * contains the data for multiple time series.  We convert to the format
 * expected by Highcharts and recreate the graph.
 */
var handleGrowthData = function(data) {

    series = {};

    _.each(data, function(record) {
        var curr_series = record["series"];
        if (!(curr_series in series)) {
            series[curr_series] = {
                "name": curr_series,
                "data": []
            };
        }
        series[curr_series]["data"].push(
            [monthStringToDate(record["month"]), record["total"]]
        );
    });

    createGoalGraph(_.values(series));
};



$(document).ready(function() {
    fetchGrowthData();
});


})();
