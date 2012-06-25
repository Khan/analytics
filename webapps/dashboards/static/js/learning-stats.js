/**
 * Script for rendering learning efficiency and retention (TODO(david)) from
 * exercises dashboard.
 */


(function() {


// TODO(david): Shared data fetcher.
var BASE_STAT_SERVER_URL = "http://184.73.72.110:27080/";
var BASE_COLLLECTION_URL = BASE_STAT_SERVER_URL +
    "report/weekly_learning_stats/";


/** @type {HighCharts.Chart|null} */
var learningGraph = null;


/**
 * Every batch request needs a unique identifier since JSONP requests can't be
 * cancelled, so we check in the handler whether this is the latest request.
 * @type {number}
 */
var resultsRequestCount = 0;


/**
 * Script entry-point called on DOM ready.
 */
var init = function init() {
    // Pre-load the "number of stacks completed" select box
    $("#stacks-select").append(_.map(_.range(1, 21), function(num) {
        return $("<option value=" + num + ">").text("exactly " + num)[0];
    }));

    learningGraph = createChart();
    addEventHandlers();
    refresh();
    getTopics();
};


// TODO(david): Use backbone?
var addEventHandlers = function addEventHandlers() {
    $("#stacks-select").change(refresh);
    $("#topics-select").change(refresh);
};


/**
 * Get topic IDs to generate learning curves for and populate select box.
 */
var getTopics = function() {
    // TODO(david): Filter out pseudo-topic "any"
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

    var $loadingBar = $("#efficiency-chart-loading .bar"),
        $chart = $("#efficiency-chart");
    $loadingBar.css("width", "10%").parent().show();
    $chart.css("opacity", 0.5);

    var numStacks = $("#stacks-select").val();
    var topic = $("#topics-select option:selected").val();

    // TODO(david): More permanent database, and design summary table with date
    //     partitions.
    var url = BASE_COLLLECTION_URL + "_find?callback=?";

    // TODO(david): Batch up requests
    var criteria = {
        num_problems_done: numStacks === "any" ? { $lte: 160 } : numStacks * 8,
        topic: topic,
        start_dt: '2012-06-13'  // TODO(david): Support date range selection
    };

    var batchSize = 1000;
    var params = {
        criteria: JSON.stringify(criteria),
        batch_size: batchSize,
        fields: JSON.stringify({
            card_number: true,
            num_deltas: true,
            sum_deltas: true
        })
    };

    var results = [];
    var numCalls = 0;

    // TODO(david): Caching. Share code with video stats.
    function getResults(url, params) {

        $.getJSON(url, params, _.bind(function(requestCount, data) {

            // Another request has taken place, abort this one
            if (requestCount !== resultsRequestCount) {
                return;
            }

            var dataResults = data["results"];
            if (dataResults && dataResults.length) {

                // Update the chart from new data
                results = results.concat(dataResults);
                results = groupByCardNumber(results);
                updateChart(results, learningGraph);
                updateSampleStats(results);

            }

            // Update UI elements
            numCalls++;
            var fakedProgress = 1 - Math.pow(0.66, numCalls);
            $loadingBar.css("width", fakedProgress.toFixed(2) * 100 + "%");
            $chart.css("opacity", 1.0);

            if (dataResults && dataResults.length === batchSize) {

                // Prepare and another batch of results
                var moreUrl = BASE_COLLLECTION_URL + "_more?callback=?";
                var moreParams = {
                    batch_size: batchSize,
                    id: data["id"]
                };
                getResults(moreUrl, moreParams);

            } else {

                // Finished loading, update UI.
                $loadingBar.parent().hide();

            }

        }, null, ++resultsRequestCount));
    }

    getResults(url, params);
    updateSampleStats(results);

};


/**
 * Aggregate rows by card number. Idempotent. This is not done through Sleepy
 * Mongoose because it may be buggy: https://jira.mongodb.org/browse/SERVER-5874
 * @param {Array.<Object>} results Rows from Mongo collection.
 * @return {Array.<Object>} Rows aggregated by card number.
 */
var groupByCardNumber = function groupByCardNumber(results) {
    return _.chain(results)
        .groupBy(function(row) { return row["card_number"]; })
        .map(function(group, cardNumber) {

            return _.chain(group)
                .reduce(function(accum, row) {
                    return {
                        sum_deltas: +accum["sum_deltas"] + +row["sum_deltas"],
                        num_deltas: +accum["num_deltas"] + +row["num_deltas"]
                    };
                })
                .extend({ card_number: cardNumber })
                .value();

        })
        .toArray()
        .value();
};


/**
 * Create learning curve HighCharts graph.
 * @return {HighCharts.Chart}
 */
var createChart = function createLearningGraph() {
    // TODO(david): Overlay charts on different segments for easy comparison
    //     (eg. num_problems_done)
    // TODO(david): Dynamically generate labels and titles
    var chartOptions = {
        chart: {
            renderTo: "efficiency-chart"
        },
        series: [{
            data: [],
            type: "areaspline",
            name: "Accumulated gain in accuracy",
            pointStart: 0  // TODO(david): Bootstrap from 1st card % correct?
        }, {
            data: [],
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
    chart.series[1].hide();  // Hide "incremental gains" series by default

    return chart;
};


/**
 * Update learning chart data from fresh results from the server.
 * @param {Array.<Object>} results Rows from the reducer summary table in mongo.
 * @param {HighCharts.Chart} chart Chart to update.
 */
var updateChart = function updateChart(results, chart) {
    var incrementalGains = _.chain(results)
        .groupByCardNumber()
        .sortBy(function(row) { return +row["card_number"]; })
        .map(function(row, index) {
            return row["sum_deltas"] / row["num_deltas"];
        })
        .value();

    var accumulatedGains = _.reduce(incrementalGains, function(accum, delta) {
        return accum.concat([_.last(accum) + delta]);
    }, [0]);

    chart.series[0].setData(accumulatedGains);
    chart.series[1].setData(incrementalGains);
};


/**
 * Update the counter of how many incremental gains this sample uses.
 * @param {Array.<Object>} results Rows from the reducer summary table in mongo.
 */
var updateSampleStats = function updateSampleStats(results) {
    // TODO(david): Show # of distinct users and error bounds
    var totalDeltas = _.reduce(results, function(accum, row) {
        return accum + +row["num_deltas"];
    }, 0);
    $("#total-deltas").text(totalDeltas);
};


// Add utility functions to underscore for convenience in chaining
_.mixin({
    groupByCardNumber: groupByCardNumber
});


$(init);


})();
