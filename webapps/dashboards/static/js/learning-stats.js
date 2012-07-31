/**
 * Script for rendering learning efficiency and retention from exercises
 * dashboard.
 */

// TODO(david): Move generic stuff out of here for others to use.


(function() {


// TODO(david): Shared data fetcher.
var BASE_DB_URL = "http://107.21.23.204:27080/report/";


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
        requestCount: 0,
        callbacksCancelled: false
    },

    initialize: function() {
        this.on("change:results", function(model, results) {
            // Compress the amount of space the results set take
            model.attributes.results = model.groupResults(results);
        });
    },

    /**
     * Reset to prepare for a new chain of batch calls.
     */
    reset: function() {
        this.set("results", []);
        this.set("numCalls", 0);
        this.set("callbacksCancelled", false);
    },

    /**
     * Get a single batch of data from the server, and get more if necessary.
     * @param {string} url The URL to send the AJAX request to
     * @param {Object} params AJAX parameters
     * @param {string} collectionUrl Base URL of the collection.
     * @param {boolean=} opt_firstCall Whether this is the first call in the
     *     chain of batch requests. Defaults to true.
     */
    fetchResults: function(url, params, collectionUrl, opt_firstCall) {

        if (opt_firstCall !== false) {
            this.reset();
        }

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

            // A new batch request has been initiated or we've been asked to
            // cancel any callbacks. Abort.
            if (requestCount !== self.get("requestCount") ||
                    self.isCallbackCancelled()) {
                return;
            }

            var dataResults = data["results"];
            if (dataResults && dataResults.length) {

                // Update with new data
                self.set("results", self.get("results").concat(dataResults));

            }

            if (dataResults && dataResults.length === self.get("batchSize")) {

                // There's probably more; fetch another batch of results
                var moreUrl = collectionUrl + "/_more?callback=?";
                var moreParams = {
                    batch_size: self.get("batchSize"),
                    id: data["id"],
                    callNum_: self.get("numCalls")
                };
                self.fetchResults(moreUrl, moreParams, collectionUrl,
                    /* opt_firstCall */ false);

            } else {

                self.trigger("allResultsLoaded");

            }

            self.set("numCalls", self.get("numCalls") + 1);

        }, null, this.get("requestCount")));

    },

    /**
     * Cancel any pending callbacks.
     */
    cancelCallbacks: function() {
        this.set("callbacksCancelled", true);
    },

    /**
     * @return {boolean} Whether any pending callbacks are to be cancelled.
     */
    isCallbackCancelled: function() {
        return this.get("callbacksCancelled");
    },

    // TODO(david): Would this be better as a static function on the class, or
    //     should it just manipulate this.attributes.results instead of taking
    //     in a param and returning?
    /**
     * Optionally override this method to compact result rows from batch calls.
     * Must be idempotent.
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
 * Model for filters (options) specifying a series.
 */
var SeriesFilters = Backbone.Model.extend({
    // Nothing here yet
});


/**
 * Abstract base class for a view of a data series.
 * FIXME(david): Properly document what to override and class-level properties
 *      to set.
 */
var SeriesView = Backbone.View.extend({

    template: Handlebars.compile($("#series-filter-template").text()),

    // TODO(david): Do interesting things on hover over a series form.
    events: {
        "change .topics-select": "onFiltersChange",
        "change .weeks-select": "onFiltersChange",
        "change .segment-select": "onFiltersChange",
        "click .close": "remove"
    },

    initialize: function(options) {

        this.chartSeries = options.chartSeries;
        this.seriesFilters = options.seriesFilters || new SeriesFilters();

        this.model
            .bind("change:results", this.updateSeries, this)
            .bind("change:numCalls", this.updateProgress, this)
            .bind("allResultsLoaded", function() {
                this.$(".request-pending-progress").hide();
            }, this);

    },

    /**
     * Override to give the template that should be inserted as series filter
     * form contents.
     * @return {function(context)} Template function to render form contents.
     */
    childTemplate: _.identity,

    /**
     * Override to give the template context.
     */
    getChildContext: _.identity,

    render: function() {
        var context = _.extend({
            seriesName: this.chartSeries.name,
            stacksOptions: _.range(1, 41)
        }, this.getChildContext());

        // This is my "poor man's" template inheritance in handlebars... the
        // "base" template has a variable in the middle called 'content'
        this.$el.html(this.template(_.extend(context, {
            content: this.childTemplate(context)
        })));

        // TODO(david): Color form background with series
        this.$("h2").css("color", this.chartSeries.color);

        this.populateTopics();
        this.populateWeeks();

        this.onFiltersChange();

        return this;
    },

    /**
     * Get topic IDs to generate learning curves for and populate select box.
     */
    populateTopics: function() {
        var self = this;
        AjaxCache.getJson("/db/learning_stats_topics", {}, function(data) {
            var options = _.chain(data["topics"])
                .without("any")
                .map(function(topic) { return $("<option>").text(topic)[0]; })
                .value();
            self.$(".topics-select").append(options);
        });
    },

    /**
     * Get start dates for weeks to populate select box.
     */
    populateWeeks: function() {
        var self = this;
        AjaxCache.getJson("/db/" + this.getCollectionName() + "/start_dates",
            {}, function(data) {
                var options = _.map(data["start_dates"], function(startDate) {
                    return $("<option>")
                        .val(startDate)
                        .text("the week of " + startDate)[0];
                });
                self.$(".weeks-select").append(options);
            });
    },

    populateSegments: function() {

        // TODO(david): Duplicated code here with SupplementalDataView.refresh
        // TODO(david): I'm eventually just going to make the UI make more sense
        //     by just making the supplemental data table the place where you
        //     specify series options.
        var topic = this.seriesFilters.get("topic");
        var startDate = this.seriesFilters.get("startDate");
        var url = BASE_DB_URL + "topic_segment_stats/_find?callback=?";

        var criteria = _.extend(
            { topic: topic },
            startDate === "any" ? {} : { start_dt: startDate }
        );

        var self = this;
        AjaxCache.getJson(url, {
            criteria: JSON.stringify(criteria),
            batch_size: 15000
        }, function(data) {
            var options = _.chain(data.results)
                .pluck("user_segment")
                .uniq()
                .map(function(val) { return $("<option>").text(val)[0]; })
                .value();

            options = [
                $("<option>any</option>")[0],
                $("(<option disabled='disabled'>---</option>)")[0]
            ].concat(options);

            // Select the previously selected segment if available
            var prevSelected = self.seriesFilters.get("segment");
            $(options)
                .filter(function() { return $(this).val() === prevSelected; })
                .attr("selected", "selected");

            self.$(".segment-select").html("").append(options);
        });

    },

    onFiltersChange: function() {
        this.seriesFilters.set({
            topic: this.$(".topics-select option:selected").val(),
            startDate: this.$(".weeks-select option:selected").val(),
            segment: this.$(".segment-select option:selected").val()
        });

        this.populateSegments();

        this.refresh();
    },

    /**
     * Ask the server for new data from user-set controls on the dashboard.
     */
    refresh: function() {

        this.$(".request-pending-progress").show();

        var topic = this.seriesFilters.get("topic");
        var startDate = this.seriesFilters.get("startDate");
        var segment = this.seriesFilters.get("segment");
        var url = this.getCollectionUrl() + "/_find?callback=?";

        // TODO(david): Support date range selection
        var criteria = _.extend({
                topic: topic
            },
            startDate === "any" ? {} : { start_dt: startDate },
            segment === "any" ? {} : { user_segment: segment },
            this.getFindCriteria());

        var params = {
            criteria: JSON.stringify(criteria),
            batch_size: this.model.get("batchSize")
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

        this.model.fetchResults(url, params, this.getCollectionUrl());

    },

    updateProgress: function() {
        var fakedProgress = 1 - Math.pow(0.66, this.model.get("numCalls"));
        fakedProgress = Math.max(0.1, fakedProgress);
        this.$(".request-pending-progress .bar")
                .css("width", fakedProgress.toFixed(2) * 100 + "%");
    },

    remove: function() {
        Backbone.View.prototype.remove.call(this);
        this.model.cancelCallbacks();
        this.chartSeries.remove();
    },

    /**
     * Must override to update UI elements that show data series info, such as
     * the graph and sample counter.
     */
    updateSeries: function() {
        throw "Not implemented";
    },

    /**
     * Must override to
     * @return {String} The Mongo collection name for this series.
     */
    getCollectionName: function() {
        throw "Not implemented";
    },

    /**
     * Optionally override to specify additional criteria to fitler the mongo
     * query by.
     * @return {Object} A map of additional filter criteria key-value pairs.
     */
    getFindCriteria: _.identity,

    /**
     * @return {string} The base URL of the Sleepy Mongoose MongoDB collection
     */
    getCollectionUrl: function() {
        return BASE_DB_URL + this.getCollectionName();
    },

    /**
     * Optionally override to specify a projection in our Mongo query.
     * @return {Array.<string>|undefined} An array of collection keys.
     */
    getCollectionFields: _.identity

}, {

    modelClass: Series

});


/**
 * View for the accuracy gain series.
 */
var AccuracyGainSeriesView = SeriesView.extend({

    events: function() {
        return _.extend({}, SeriesView.prototype.events, {
            "change .stacks-select": "onFiltersChange",
            "click .comparison-op": "changeComparison"
        });
    },

    /** @override */
    onFiltersChange: function() {
        var numStacks = this.$(".stacks-select").val();
        this.$(".comparison-op").toggle(numStacks !== "any");
        this.seriesFilters.set("numStacks", numStacks);

        var operator = this.$(".comparison-op").text();
        this.seriesFilters.set("comparisonOp", operator);

        SeriesView.prototype.onFiltersChange.call(this);
    },

    changeComparison: function() {
        var $op = this.$(".comparison-op");
        var operator = $op.text();
        var choices = ["exactly", "at least", "no more than"];
        var next = choices[(_.indexOf(choices, operator) + 1) % choices.length];
        $op.text(next);
        this.seriesFilters.set("comparisonOp", next);
        this.refresh();
    },

    /** @override */
    childTemplate: Handlebars.compile($("#accuracy-gain-template").text()),

    /** @override */
    getChildContext: function() {
        // TODO(david): It would be nice if handlebars supported template
        //     inheritance... then we don't have to stick presentation details
        //     here in the JS.
        return {
            sampleType: "incremental gains"
        };
    },

    /** @override */
    getFindCriteria: function() {
        var numStacks = this.seriesFilters.get("numStacks");
        var operator = this.seriesFilters.get("comparisonOp");

        var numDoneVal = {};
        if (numStacks === "any") {
            numDoneVal['$lte'] = 160;
        } else {
            var numCards = numStacks * 8;  // TODO(david): Not always true
            numDoneVal = {
                "exactly": numCards,
                "at least": { "$gte": numCards },
                "no more than": { "$lte": numCards }
            }[operator];
        }

        return {
            num_problems_done: numDoneVal
        };
    },

    /** @override */
    getCollectionName: function() {
        return "weekly_learning_stats";
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
        this.$(".total-samples").text(totalDeltas);

    }

}, {

    modelClass: AccuracyGainSeries,

    seriesName: "Accuracy Gain",

    seriesOptions: {
        // TODO(david): Bootstrap from 1st card % correct?
        pointStart: 0
    },

    yAxis: {
        title: { text: "Accumulated gain in accuracy" }
    }

});


/**
 * View for the topic retention series.
 */
var UsersSeriesView = SeriesView.extend({

    events: function() {
        return _.extend({}, SeriesView.prototype.events, {
            "change .users-select": "updateSeries",
        });
    },

    /** @override */
    childTemplate: Handlebars.compile($("#retention-template").text()),

    /** @override */
    getChildContext: function() {
        return {
            sampleType: "total attempts"
        };
    },

    /** @override */
    getCollectionName: function() {
        return "topic_retention_stats";
    },

    /** @override */
    getCollectionFields: function() {
        return ["bucket_value", "num_attempts"];
    },

    /** @override */
    updateSeries: function() {

        // TODO(david): Better highcharts: max at 1.0 when percent, min 0, etc.

        var results = this.model.get("results");

        var yaxisType = this.$(".users-select option:selected").val();
        var normalizer = 1;
        if (yaxisType === "percent") {
            normalizer = _.chain(results)
                .pluck("num_attempts")
                .max()
                .value();
            normalizer = Math.max(1, normalizer);
        }

        var numAttemptsSeries = _.chain(results)
            .sortBy(function(row) { return +row["bucket_value"]; })
            .map(function(row) { return row["num_attempts"] / normalizer; })
            .value();

        this.chartSeries.setData(numAttemptsSeries);

        // TODO(david): A bit of duplicated code here
        var totalAttempts = _.reduce(results, function(accum, row) {
            return accum + +row["num_attempts"];
        }, 0);
        this.$(".total-samples").text(totalAttempts);

    }

}, {

    seriesName: "User retention",

    seriesOptions: {
        type: "spline",
        pointStart: 1,
        marker: {
            enabled: true
        }
    },

    yAxis: {
        title: { text: "User retention" },
        min: 0
    }

});


/**
 * View for the percent correct series.
 */
var PercentCorrectSeriesView = SeriesView.extend({

    /** @override */
    childTemplate: Handlebars.compile($("#percent-correct-template").text()),

    /** @override */
    getChildContext: function() {
        return {
            sampleType: "total attempts"
        };
    },

    /** @override */
    getCollectionName: function() {
        return "topic_retention_stats";
    },

    /** @override */
    getCollectionFields: function() {
        return ["bucket_value", "num_attempts", "num_correct"];
    },

    /** @override */
    updateSeries: function() {

        var results = this.model.get("results");

        var numAttemptsSeries = _.chain(results)
            .sortBy(function(row) { return +row["bucket_value"]; })
            .map(function(row) { return row["num_correct"] / row["num_attempts"]; })
            .value();

        this.chartSeries.setData(numAttemptsSeries);

        // TODO(david): A bit of duplicated code here
        var totalAttempts = _.reduce(results, function(accum, row) {
            return accum + +row["num_attempts"];
        }, 0);
        this.$(".total-samples").text(totalAttempts);

    }

}, {

    seriesName: "Percent correct",

    seriesOptions: {
        type: "spline",
        pointStart: 1,
    },

    yAxis: {
        title: { text: "Percent correct" },
        min: 0,
        max: 1
    }

});


/**
 * View of supplemental data for a topic-segment presented in a table row.
 */
var SupplementalDataView = Backbone.View.extend({

    template: Handlebars.compile($("#supplemental-data-row").text()),

    initialize: function(options) {
        this.chartSeries = options.chartSeries;
        this.seriesFilters = options.seriesFilters;

        this.model.on("change:results", this.updateData, this);
        this.seriesFilters.on("change", this.refresh, this);
    },

    render: function() {
        this.refresh();
    },

    refresh: function() {
        var topic = this.seriesFilters.get("topic");
        var startDate = this.seriesFilters.get("startDate");
        var segment = this.seriesFilters.get("segment");
        var collectionUrl = BASE_DB_URL + "topic_segment_stats";
        var url = collectionUrl + "/_find?callback=?";

        // TODO(david): Support date range selection
        var criteria = _.extend(
            { topic: topic },
            startDate === "any" ? {} : { start_dt: startDate },
            segment === "any" ? {} : { user_segment: segment }
        );

        var params = {
            criteria: JSON.stringify(criteria),
            batch_size: 15000
        };

        this.model.fetchResults(url, params, collectionUrl);
    },

    updateData: function() {
        // TODO(david): Aggregate results rows when we get multiple rows. Will
        //     do this when we actually get multiple segment data (custom stack
        //     launch) to test/play with.
        var results = this.model.get("results")[0];
        if (!results) {
            return;
        }

        var $series = $("<span>")
            // Strip everything including and after the first hyphen
            .text(/^[^-]*/.exec(this.chartSeries.name)[0])
            .css("color", this.chartSeries.color);

        var context = {
            series: $series.wrap("<p>").parent().html(),  // Outer HTML
            topic: results.topic,
            startDate: results.start_dt,
            segment: results.user_segment,
            users: results.all_users,
            cardsPerUser: results.all_attempts / results.all_users,
            minutesPerUser: results.total_time / 60 / results.all_users,
            gainPerUser: results.sum_deltas_randomized /
                results.random_card_users,
            firstCardAccuracyAll: results.first_card_correct_all /
                results.first_card_attempts_all,
            firstCardAccuracyRandomized: results.first_card_correct_randomized /
                results.first_card_attempts_randomized,
            allCardsAccuracy: results.num_correct_all /
                results.num_attempts_all,
            randomizedCardsAccuracy: results.num_correct_randomized /
                results.num_attempts_randomized,
            totalGain: results.sum_deltas_randomized
        };

        this.$el.html(this.template(context));
    }

});


/**
 * View of the entire dashboard.
 */
var DashboardView = Backbone.View.extend({

    // TODO(david): This will be changed once more chart types are added.
    el: "body",

    events: {
        "click #add-series-buttons .btn": "addSeriesHandler"
    },

    initialize: function() {
        this.chart = this.createChart();
        this.addSeries(UsersSeriesView);
    },

    /**
     * Create learning curve HighCharts graph.
     * @return {HighCharts.Chart}
     */
    createChart: function() {

        // Highcharts can't dynamically add axes so pre-fill with empty axes
        var emptyAxes = _.map(_.range(0, 20), function(index) {
            return { title: { text: null }, opposite: !!(index % 2) };
        });

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
            yAxis: emptyAxes,
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

    /**
     * Click event handler to add a new series.
     * @param {Object} event
     */
    addSeriesHandler: function(event) {
        event.preventDefault();

        var seriesType = $(event.currentTarget).data("series");
        var seriesConstructor = {
            gain: AccuracyGainSeriesView,
            users: UsersSeriesView,
            percentCorrect: PercentCorrectSeriesView
        }[seriesType];

        this.addSeries(seriesConstructor);
    },

    /**
     * Add a series to the dashboard.
     * @param {function} seriesViewConstructor A constructor to create the
     *     desired series.
     */
    addSeries: function(seriesViewConstructor) {

        var seriesNum = DashboardView.numSeries++;
        var axisOptions = seriesViewConstructor.yAxis || {};
        var seriesName = "Series " + (seriesNum + 1) + " - " +
            seriesViewConstructor.seriesName;

        var yAxisIndex =_.indexOf(DashboardView.yAxesNames,
            seriesViewConstructor.seriesName);
        if (yAxisIndex === -1) {
            yAxisIndex = DashboardView.yAxesNames.length;
            DashboardView.yAxesNames.push(seriesViewConstructor.seriesName);
        }

        this.chart.addSeries(_.extend({
            data: [],
            type: "areaspline",
            name: seriesName,
            yAxis: yAxisIndex
        }, seriesViewConstructor.seriesOptions));

        // Unfortunately, HighCharts 2.2.5 has a bug where calling addSeries
        // does not return the series added.
        var chartSeries = _.find(this.chart.series, function(series) {
            return series.name === seriesName;
        });

        var yAxis = this.chart.yAxis[yAxisIndex];
        if (!yAxis.options.title || !yAxis.options.title.text) {
            // Unfortunately, Highcharts 2.2.5 has a bug where setting the title
            // again will not erase the old one.
            yAxis.setTitle(axisOptions.title);
        }
        if (_.has(axisOptions, 'min') || _.has(axisOptions, 'max')) {
            yAxis.setExtremes(axisOptions.min, axisOptions.max);
        }

        var seriesFilters = new SeriesFilters();

        var seriesView = new seriesViewConstructor({
            el: $("<div>").appendTo("#series-forms"),
            model: new seriesViewConstructor.modelClass(),
            chartSeries: chartSeries,
            seriesFilters: seriesFilters
        });
        seriesView.render();

        var supplementalDataView = new SupplementalDataView({
            el: $("<tr>").appendTo("#supplemental-data-table tbody"),
            model: new Series(),
            chartSeries: chartSeries,
            seriesFilters: seriesFilters
        });
        supplementalDataView.render();

    }

}, {

    numSeries: 0,

    yAxesNames: []

});


/**
 * Register all handlebars partials on the page.
 */
var registerHandlebarsPartials = function registerHandlebarsPartials() {
    $("#handlebars-partials script[type='text/x-handlebars-template']").each(
        function(index, elem) {
            var $elem = $(elem);
            Handlebars.registerPartial($elem.attr("id"), $elem.text());
        }
    );
};


$(function() {
    registerHandlebarsPartials();
    var dashboard = new DashboardView();
});


})();
