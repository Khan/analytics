/**
 * Script for rendering learning efficiency and retention (TODO(david)) from
 * exercises dashboard.
 */

// TODO(david): Move generic stuff out of here for others to use.


(function() {


// TODO(david): Shared data fetcher.
var BASE_STAT_SERVER_URL = "http://184.73.72.110:27080/";


// TODO(david): Do I need to document in the JSDoc here what method can be
//     overriden and what must be overriden?
/**
 * A single data series to show on a chart.
 */
var Series = Backbone.Model.extend({

    defaults: {
        results: [],
        numCalls: 0,
        batchSize: 1000,
        requestCount: 0
    },

    /**
     * Reset to prepare for a new chain of batch calls.
     */
    reset: function() {
        this.set("results", []);
        this.set("numCalls", 0);
    },

    /**
     * Get a single batch of data from the server, and get more if necessary.
     * @param {string} url The URL to send the AJAX request to
     * @param {Object} params AJAX parameters
     * @param {string} collectionUrl Base URL of the collection, ending in /
     */
    fetchResults: function(url, params, collectionUrl) {

        var self = this;
        this.set("requestCount", this.get("requestCount") + 1);

        // TODO(david) FIXME: Another series could send off a request with the
        //     same cursor ID, since unfortunately Sleepy Mongoose does not have
        //     a stateless RESTful API (it's not a cursor ID, it's an ID of the
        //     query:
        //     github.com/kchodorow/sleepy.mongoose/wiki/Getting-More-Results ).
        //     This leads to _more responses with the same ID being interleaved
        //     among both senders.
        AjaxCache.getJson(url, params, _.bind(function(requestCount, data) {

            // A new batch request has been initiated, abort this one
            if (requestCount !== self.get("requestCount")) {
                return;
            }

            var dataResults = data["results"];
            if (dataResults && dataResults.length) {

                // Update with new data
                var results = self.get("results");
                results = results.concat(dataResults);
                self.set("results", self.groupResults(results));

            }

            if (dataResults && dataResults.length === self.get("batchSize")) {

                // There's probably more; fetch another batch of results
                var moreUrl = collectionUrl + "_more?callback=?";
                var moreParams = {
                    batch_size: self.get("batchSize"),
                    id: data["id"],
                    callNum_: self.get("numCalls")
                };
                self.fetchResults(moreUrl, moreParams, collectionUrl);

            } else {

                self.trigger("allResultsLoaded");

            }

            self.set("numCalls", self.get("numCalls") + 1);

        }, null, this.get("requestCount")));

    },

    // TODO(david): Would this be better as a static function on the class, or
    //     should it just manipulate this.attributes.results instead of taking
    //     in a param and returning?
    /**
     * Override this method to compact result rows from batch calls. Must be
     * idempotent.
     * @param {Array.<Object>} results Rows from Mongo collection.
     * @return {Array.<Object>} Grouped rows.
     */
    groupResults: _.identity

});


/**
 * A series for accuracy gains.
 */
var AccuracyGainSeries = Series.extend({

    /**
     * Aggregate rows by card number. Idempotent. Not done through Sleepy
     * Mongoose because it may be buggy:
     * https://jira.mongodb.org/browse/SERVER-5874
     * @param {Array.<Object>} results Rows from Mongo collection.
     * @return {Array.<Object>} Rows aggregated by card number.
     */
    groupResults: function(results) {
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
    }

});


/**
 * Abstract base class for a view of a data series.
 */
var SeriesView = Backbone.View.extend({

    // TODO(david): Do interesting things on hover over a series form.
    events: {
        "change .topics-select": "refresh"
    },

    initialize: function(options) {

        this.chartSeries = options.chartSeries;

        this.model
            .bind("change:results", this.updateSeries, this)
            .bind("change:numCalls", this.updateProgress, this)
            .bind("allResultsLoaded", function() {
                this.$el.find(".request-pending-progress").hide();
            }, this);

    },

    render: function() {
        this.$el.html(this.template({
            seriesName: this.chartSeries.name,
            stacksOptions: _.range(1, 21)
        }));

        // TODO(david): Color form background with series
        this.$el.find('h2').css('color', this.chartSeries.color);

        this.populateTopics();

        return this;
    },

    /**
     * Get topic IDs to generate learning curves for and populate select box.
     */
    populateTopics: function() {
        var self = this;
        // TODO(david): Filter out pseudo-topic "any"
        AjaxCache.getJson("/db/learning_stats_topics", {}, function(data) {
            var options = _.map(data["topics"], function(topic) {
                return $("<option>").text(topic)[0];
            });
            self.$el.find(".topics-select").append(options);
        });
    },

    /**
     * Ask the server for new data from user-set controls on the dashboard.
     */
    refresh: function() {

        this.$el.find(".request-pending-progress").show();

        var topic = this.$el.find(".topics-select option:selected").val();
        var url = this.getCollectionUrl() + "_find?callback=?";

        // TODO(david): Support date range selection
        var criteria = _.extend({
            topic: topic
        }, this.getFindCriteria());

        var params = {
            criteria: JSON.stringify(criteria),
            batch_size: this.model.get("batchSize"),
        };

        // Maybe specify a projection for this query
        var fields = this.getCollectionFields();
        if (fields) {
            var fieldsObject = _.reduce(fields, function(accum, value) {
                accum[value] = value;
                return accum;
            }, {});
            params.fields = JSON.stringify(fieldsObject);
        }

        this.model.reset();
        this.model.fetchResults(url, params, this.getCollectionUrl());

    },

    updateProgress: function() {
        var fakedProgress = 1 - Math.pow(0.66, this.model.get("numCalls"));
        fakedProgress = Math.max(0.1, fakedProgress);
        this.$el.find(".request-pending-progress .bar")
                .css("width", fakedProgress.toFixed(2) * 100 + "%");
    },

    /**
     * Must override to update UI elements that show data series info, such as
     * the graph and sample counter.
     */
    updateSeries: function() {
        throw "Not implemented";
    },

    /**
     * Optionally override to specify additional criteria to fitler the mongo
     * query by.
     * @return {Object} A map of additional filter criteria key-value pairs.
     */
    getFindCriteria: function() {
        return {};
    },

    /**
     * Optionally override to
     * @return {string} The base URL of the Sleepy Mongoose MongoDB collection
     */
    getCollectionUrl: _.identity,

    /**
     * Optionally override to specify a projection in our Mongo query.
     * @return {Array.<string>|undefined} An array of collection keys.
     */
    getCollectionFields: _.identity

});


/**
 * View for the accuracy gain series.
 */
var AccuracyGainSeriesView = SeriesView.extend({

    // TODO(david): Handlebars template should inherit from base
    template: Handlebars.compile($("#accuracy-gain-form-template").text()),

    events: function() {
        return _.extend({}, SeriesView.prototype.events, {
            "change .stacks-select": "refresh",
        });
    },

    /** @override */
    getFindCriteria: function() {
        var numStacks = this.$el.find(".stacks-select").val();
        return {
            num_problems_done: numStacks === "any" ? { $lte: 160 } :
                numStacks * 8,
        };
    },

    /** @override */
    getCollectionUrl: function() {
        return BASE_STAT_SERVER_URL + "report/weekly_learning_stats/";
    },

    /** @override */
    getCollectionFields: function() {
        return ["card_number", "sum_deltas", "num_deltas"];
    },

    /** @override */
    updateSeries: function() {

        var results = this.model.get("results");
        results = this.model.groupResults(results);

        var incrementalGains = _.chain(results)
            .sortBy(function(row) { return +row["card_number"]; })
            .map(function(row, index) {
                return row["sum_deltas"] / row["num_deltas"];
            })
            .value();

        var accumulatedGains = _.reduce(incrementalGains,
                function(accum, delta) {
            return accum.concat([_.last(accum) + delta]);
        }, [0]);

        this.chartSeries.setData(accumulatedGains);
        // TODO(david): Restore displaying of incremental gains

        // TODO(david): Show # of distinct users and error bounds
        var totalDeltas = _.reduce(results, function(accum, row) {
            return accum + +row["num_deltas"];
        }, 0);
        this.$el.find(".total-deltas").text(totalDeltas);

    }

}, {

    modelClass: AccuracyGainSeries,

    seriesOptions: {
        // TODO(david): Bootstrap from 1st card % correct?
        pointStart: 0
    }

});


/**
 * View of the entire dashboard.
 */
var DashboardView = Backbone.View.extend({

    // TODO(david): This will be changed once more chart types are added.
    el: "body",

    events: {
        "click #compare-button": "addSeries"
    },

    initialize: function() {
        this.chart = this.createChart();
        this.addSeries();
    },

    /**
     * Create learning curve HighCharts graph.
     * @return {HighCharts.Chart}
     */
    createChart: function() {
        // TODO(david): Dynamically generate labels and titles
        var chartOptions = {
            chart: {
                renderTo: "highcharts-graph"
            },
            plotOptions: {
                series: {
                    marker: {
                        enabled: false,
                        states: {
                            hover: {
                                enabled: true
                            }
                        }
                    }
                }
            },
            series: [],
            title: {
                text: null
            },
            // TODO(david): Multiple y-axes
            yAxis: {
                title: { text: "Accumulated gain in accuracy" },
            },
            xAxis: {
                title: { text: "Card Number" },
                min: 0,
                max: 100
            },
            credits: { enabled: false }
        };

        var chart = new Highcharts.Chart(chartOptions);

        return chart;
    },

    addSeries: function() {

        // TODO(david): Add different type of series
        var seriesViewConstructor = AccuracyGainSeriesView;

        var seriesNum = ++DashboardView.numSeries;
        var chartSeries = this.chart.addSeries(_.extend({
            data: [],
            type: "areaspline",
            name: "Series " + seriesNum,
        }, seriesViewConstructor.seriesOptions));

        var series = new seriesViewConstructor.modelClass();
        var seriesView = new seriesViewConstructor({
            el: $("<div>").appendTo("#series-forms"),
            model: series,
            chartSeries: chartSeries
        });
        seriesView.render().refresh();

    }

    // TODO(david): Button to remove series

}, {

    numSeries: 0

});


$(function() {
    var dashboard = new DashboardView();
});


})();
