/**
 * Script for rendering learning efficiency and retention (TODO(david)) from
 * exercises dashboard.
 */


(function() {


// TODO(david): Shared data fetcher.
var BASE_STAT_SERVER_URL = "http://184.73.72.110:27080/";
var BASE_COLLLECTION_URL = BASE_STAT_SERVER_URL +
    "report/weekly_learning_stats/";


// TODO(david): Caching. Share code with video stats.


/**
 * Script entry-point called on DOM ready.
 */
var init = function init() {
    addEventHandlers();
    refresh();
    getTopics();
};


// TODO(david): Use backbone?
var addEventHandlers = function addEventHandlers() {
    $("#stacks-select").change(refresh);
    $("#topics-select").change(refresh);
};


var getTopics = function() {
    $.get("/db/learning_stats_topics", function(data) {
        var options = _.map(data["topics"], function(topic) {
            return $("<option>").text(topic)[0];
        });
        $("#topics-select").append(options);
    });
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
    var topic = $("#topics-select option:selected").val();

    // TODO(david): More permanent database, and design summary table with date
    //     partitions.
    var url = BASE_COLLLECTION_URL + "_find?callback=?";

    // TODO(david): Batch up requests
    var criteria = {
        num_problems_done: "" + (numStacks * 8)
    };
    if (topic !== "any") {
        criteria["topic"] = topic;
    }

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

    // TODO(david): Show # of distinct users and error bounds
    var totalDeltas = _.reduce(results, function(accum, row) {
        return accum + +row["num_deltas"];
    }, 0);
    $("#total-deltas").text(totalDeltas);
};


$(init);


})();
