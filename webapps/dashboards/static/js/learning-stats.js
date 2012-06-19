/**
 * Script for rendering learning efficiency and retention (TODO(david)) from
 * exercises dashboard.
 */


(function() {


// TODO(david): Shared data fetcher.
var BASE_STAT_SERVER_URL = "http://184.73.72.110:27080/";


/**
 * Script entry-point called on DOM ready.
 */
var init = function init() {
    addEventHandlers();
    refresh();
};


// TODO(david): Use backbone?
var addEventHandlers = function addEventHandlers() {
    $('#stacks-select').change(refresh);
};


/**
 * Regenerate the chart from user-set controls on the dashboard by asking server
 * for new data and then rendering.
 */
var refresh = function refresh() {
    var $loadingBar = $("#efficiency-chart-loading"),
        $chart = $("#efficiency-chart");
    $loadingBar.show();
    $chart.css("opacity", 0.5);

    var numStacks = $("#stacks-select").val();

    // TODO(david): More permanent database, and design summary table with date
    //     partitions.
    var url = BASE_STAT_SERVER_URL + "test/accuracy_deltas/_find?callback=?";
    // TODO(david): Batch up requests
    var criteria = {
        num_problems_done: "" + (numStacks * 8)
    };
    // TODO(david): Specify just those fields we want from the server.
    var params = {
        criteria: JSON.stringify(criteria),
        batch_size: 100
    };

    // TODO(david): JSON data from mongo should be properly typed.
    $.getJSON(url, params, function(data) {
        renderChart(data["results"]);
        $loadingBar.hide();
        $chart.css("opacity", 1.0);
    });
};


/**
 * Render highcharts.
 * @param {Array.<Object>} results Rows from the reducer summary table in mongo.
 */
var renderChart = function renderChart(results) {
    // TODO(david): Don't need the uniq once I specify an ID row in mongo.
    var incrementalGains = _.chain(results)
        .sortBy(function(row) { return +row["card_number"]; })
        .uniq(/* isSorted */ true, function(row) {
            return row["card_number"];
        })
        .map(function(row, index) {
            return +row["avg_deltas"];
        })
        .value();

    var accumulatedGains = _.reduce(incrementalGains, function(accum, delta) {
        return accum.concat([_.last(accum) + delta]);
    }, [0]);

    // TODO(david): Overlay charts on different segments for easy comparison
    //     (eg. num_problems_done)
    // TODO(david): Dynamically generate labels and titles
    var chartOptions = {
        chart: {
            renderTo: "efficiency-chart"
        },
        series: [{
            data: accumulatedGains,
            type: "areaspline",
            name: "Accumulated gain in accuracy",
            pointStart: 0  // TODO(david): Bootstrap from 1st card % correct?
        }, {
            data: incrementalGains,
            type: "spline",
            name: "Incremental gain in accuracy",
            pointStart: 1
        }],
        title: {
            text: "Gain in accuracy over number of cards done"
        },
        yAxis: {
            title: { text: "Gain in accuracy" },
        },
        xAxis: {
            title: { text: "Card Number" }
        },
        credits: { enabled: false }
    };

    var chart = new Highcharts.Chart(chartOptions);
};


$(init);


})();
