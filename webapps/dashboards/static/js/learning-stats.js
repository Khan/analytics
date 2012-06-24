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
    // Pre-load the "number of stacks completed" select box
    $("#stacks-select").append(_.map(_.range(1, 21), function(num) {
        return $("<option value=" + num + ">").text("exactly " + num)[0];
    }));

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
        num_problems_done: numStacks === "any" ? { $lte: 160 } : numStacks * 8,
        topic: topic,
        start_dt: '2012-06-13'  // TODO(david): Support date range selection
    };

    var params = {
        criteria: JSON.stringify(criteria),
        batch_size: 20000,
        fields: JSON.stringify({
            card_number: true,
            num_deltas: true,
            sum_deltas: true
        })
    };

    $.getJSON(url, params, function(data) {
        renderChart(data["results"]);
        $loadingBar.hide();
        $chart.css("opacity", 1.0);
    });
};


/**
 * Aggregate rows by card number. This is not done through Sleepy Mongoose
 * because it seems it may be buggy: https://jira.mongodb.org/browse/SERVER-5874
 * @param {Array.<Object>} results Rows from Mongo collection.
 * @return {Object} A map of card number to the corresponding aggregated row.
 */
var groupByCardNumber = function groupByCardNumber(results) {
    return _.chain(results)
        .groupBy(function(row) { return row["card_number"]; })
        .map(function(group, cardNumber) {
            return _.reduce(group, function(accum, row) {
                return {
                    sum_deltas: +accum["sum_deltas"] + +row["sum_deltas"],
                    num_deltas: +accum["num_deltas"] + +row["num_deltas"]
                };
            });
        })
        .value();
};


/**
 * Render highcharts.
 * @param {Array.<Object>} results Rows from the reducer summary table in mongo.
 */
var renderChart = function renderChart(results) {
    var incrementalGains = _.chain(results)
        .groupByCardNumber()
        // _.toArray() seems to do the same but just in case and to be explicit
        .sortBy(function(value, key) { return +key; })
        .map(function(row, index) {
            return row["sum_deltas"] / row["num_deltas"];
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
    chart.series[1].hide();  // Hide "incremental gains" series by default

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
