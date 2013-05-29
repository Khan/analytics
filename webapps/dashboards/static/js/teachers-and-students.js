/**
 * Dashboard for teacher and student growth metrics.
 */

(function() {

    // Database URLs
    var BASE_DB_URL = "http://107.21.23.204:27080/report/";
    var COLLECTION_FIND_URL = "student_teacher_count/_find?callback=";
    var QUERY_URL = BASE_DB_URL + COLLECTION_FIND_URL;
    var onReady = $.Deferred();

    // Model for data series
    var CountEntryModel = Backbone.Model.extend({
        defaults: {
            dt: "",
            teacher_count: 0,
            student_count: 0,
            active_teacher_count: 0,
            active_student_count: 0
        },
        cid: "dt",

        parse: function(response) {
            return response.results;
        },

        url: ""
    });

    // Collection keeping the models
    var CountSeriesCollection = Backbone.Collection.extend({
        model: CountEntryModel,

        parse: function(response) {
            return _.map(response.results, function(dataPoint) {
                return new CountEntryModel(dataPoint);
            });
        },

        url: function() {
            return QUERY_URL + "?&batch_size=15000";
        }
    });

    /**
     * Initialize the graph with its formatting options.
     */
    var createGraph = function(dataSeries, renderTo, title) {
        var chartOptions = {
            chart: {
                zoomType: "x",
                type: "spline",
                spacingRight: 20
            },
            title: {
                text: title
            },
            xAxis: {
                type: "datetime",
                minRange: 7 * 24 * 3600000,
                dateTimeLabelFormats: { day: "%a %e %b" }
            },
            yAxis: {
                min: -1000,
                startOnTick: false,
                title: { text: "" }
            },
            series: dataSeries,
            plotOptions: {
                spline: {
                    shadow: true,
                    marker: {
                        enabled: false
                    },
                }
            },
            credits: { enabled: false }
        };

        $(renderTo).highcharts(chartOptions);
    };


    /**
     * Process the response for a query of the mongo REST API.  The response
     * contains the data for multiple time series.  We convert to the format
     * expected by Highcharts and recreate the graph.
     */
    var handleGrowthData = function(data) {
        onReady.done(function() {
            teacherSeries = {
                "teachers": {
                    "name": "teachers",
                    "data": []
                },
                "active_teachers": {
                    "name": "active teachers",
                    "data": []
                }
            };

            studentSeries = {
                "students": {
                    "name": "students",
                    "data": []
                },
                "active_students": {
                    "name": "active students",
                    "data": []
                }
            };

            _.chain(data.models)
            .pluck("attributes")
            .sortBy("dt")
            .each(function(record) {
                var epochTime = moment(record["dt"], "YYYY-MM-DD").valueOf();
                teacherSeries["teachers"]["data"].push(
                    [epochTime, record["teacher_count"]]
                );
                studentSeries["students"]["data"].push(
                    [epochTime, record["student_count"]]
                );
                teacherSeries["active_teachers"]["data"].push(
                    [epochTime, record["active_teacher_count"]]
                );
                studentSeries["active_students"]["data"].push(
                    [epochTime, record["active_student_count"]]
                );
            }).value();

            createGraph(_.values(teacherSeries), "#teacher-graph-container",
                "Number of teachers by day");
            createGraph(_.values(studentSeries), "#student-graph-container",
                "Number of students by day");
        });
    };

    // Kick everything off by creating and populating collection
    var dataCollection = new CountSeriesCollection();

    dataCollection.on("reset", handleGrowthData);

    dataCollection.fetch({
        cache: true,
        reset: true,
        ttl: 12
    });

    $(function() {
        // Draw graphs when DOM is loaded
        onReady.resolve();
    });

})();
