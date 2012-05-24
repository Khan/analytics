/**
 * Logic for fetching data and rendering the daily exercise
 * statistics dashboard.
 */

var exChartOptions = {
    chart: {
        renderTo: "ex_chart_container",
        defaultSeriesType: "spline"
    },
    title: {
        text: "Exercise over time"
    },
    xAxis: {
        type: "datetime",
        dateTimeLabelFormats: { day: "%a %e %b" }
    },
    yAxis: {
        title: { text: "" }
    },
    series: []
};

var modeChartOptions = {
    chart: {
        renderTo: "mode_chart_container",
        defaultSeriesType: "spline"
    },
    title: {
        text: "Mode stats over time"
    },
    xAxis: {
        type: "datetime",
        dateTimeLabelFormats: { day: "%a %e %b" }
    },
    yAxis: {
        title: { text: "" }
    },
    series: []
};

$(document).ready(function() {
    var BUTTON_TEXT_COMPLICATE = "I'm not nearly confused enough.  Give me more power!";
    var BUTTON_TEXT_SIMPLIFY = "Too. Much. Power.  Make it simpler!";

    var BASE_STAT_SERVER_URL = "http://184.73.72.110:28017/";

    var chart;
    var setExChart = function() {

        var exName = $("#exerciselist").val();
        var statName = $("#statlist").val();
        var normName = $("#normlist").val();
        var superMode = $("#superlist").val();
        var tmodes = $("#filterlist").val();
        var statDesc = statName + ((normName == "none") ? "" : (" divided by " + normName));

        // load all data from the collection
        var series = {};

        var chartSeries = [],
            chartTitles = [];

        if (!tmodes instanceof Array) {
            tmodes = [tmodes];
        }

        var deferreds = [];
        $.each(tmodes, function(ix, tmode) {
            var params = {
                filter_exercise: exName,
                filter_filter_mode: tmode,
                filter_super_mode: superMode
            };

            // jQuery uses "jsonp=?" as a special indicator to use an
            // auto-generated JSONP callback.
            var url = BASE_STAT_SERVER_URL + "report/daily_ex_stats/?jsonp=?";
            deferreds[deferreds.length] = $.getJSON(url, params, function(sdata) {

                series = sdata;

                // this function takes the result of the mongodb simple REST api
                // and extracts data in the format expected by highcharts, specifically
                // [[date1,val1], [date2, val2], ...]
                var getData = function(series, dateField) {
                    var data = [];
                    var rows = series["rows"];
                    $.each(rows, function(exName, row) {
                        var denom = 1.0;
                        if (normName != "none" && normName in row) {
                            denom = row[normName];
                        }
                        if (dateField in row && statName in row) {
                            data.push([row[dateField]["$date"], row[statName] / denom]);
                        }
                    });
                    return data;
                };

                var seriesName = statName + " (filter mode: " + tmode + ")";
                chartSeries[chartSeries.length] = { name: seriesName, data: getData(series, "date", statName) };
                chartTitles[chartTitles.length] = exName + ": " + statDesc + " over time";

            });

        });

        $.when.apply(null, deferreds).done(function() {

            exChartOptions.series = chartSeries;
            exChartOptions.title.text = chartTitles[0];

            chart = new Highcharts.Chart(exChartOptions);

        });

    };

    // this version of the mode chart shows the prevelance of a certain submode within the list of selected filters
    var setModeChartComparison = function() {

        if (!$("#mode_chart_container").is(":visible")) {
            return;
        }

        var superMode = $("#superlist").val();
        var tmodes = $("#filterlist").val();
        if (!tmodes instanceof Array) {
            tmodes = [tmodes];
        }
        var subMode = $("#compositionlist").val();
        var usePercentages = ($("#formatlist").val() == "percentage");

        // load all data from the collection
        var series = {};

        var chartSeries = [],
            chartTitles = [];

        var deferreds = [];
        $.each(tmodes, function(ix, tmode) {
            var params = {
                filter_filter_mode: tmode,
                filter_super_mode: superMode
            };

            var url = BASE_STAT_SERVER_URL + "report/daily_ex_mode_stats/?jsonp=?";

            deferreds[deferreds.length] = $.getJSON(url, params, function(sdata) {
                series = sdata;
                console.log(series);
                // this function takes the result of the mongodb simple REST api
                // and extracts data in the format expected by highcharts, specifically
                // [[date1,val1], [date2, val2], ...]
                var getData = function(series, dateField) {
                    var data = [];
                    var rows = series["rows"];
                    $.each(rows, function(exName, row) {
                        var denom = 1.0;
                        if (usePercentages) {
                            denom = row["count"] + 0.1;
                        }
                        if (dateField in row && subMode in row) {
                            data.push([row[dateField]["$date"], row[subMode] / denom]);
                        }
                    });
                    return data;
                };

                chartSeries[chartSeries.length] = { name: subMode + " (" + tmode + ")", data: getData(series, "date", subMode) };
                chartTitles[chartTitles.length] = "Prevalance of " + subMode + " in selected segments over time";

            });
        });

        $.when.apply(null, deferreds).done(function() {

            modeChartOptions.series = chartSeries;
            modeChartOptions.title.text = chartTitles[0];

            chart = new Highcharts.Chart(modeChartOptions);

        });


    };

    // this version of the mode chart shows the complete submode composition within the top selected filter
    var setModeChartComposition = function() {

        if (!$("#mode_chart_container").is(":visible")) {
            return;
        }

        var superMode = $("#superlist").val();
        var tmode = $("#filterlist").val();
        if (tmode instanceof Array && tmode.length > 0) {
            tmode = tmode[0];
        }

        var params = {
            filter_exercise: exName,
            filter_filter_mode: tmode,
            filter_super_mode: superMode
        };
        var url = BASE_STAT_SERVER_URL + "report/daily_ex_mode_stats/?jsonp=?";

        $.getJSON(url, params, function(sdata) {

            var seriesSet = [];
            var query_rows = sdata["rows"];
            $.each(query_rows, function(exName, row) {
                for (var property in row) {
                    if (!row.hasOwnProperty(property)) { continue; }
                    //TODO(jace): 'some' must be a reserved word
                    if (property in {"_id": "", "count": "", "filter_name": "", "date": "", "some": ""}) { continue; }
                    if (!(property in seriesSet)) {
                        seriesSet[property] = { "name": property, "data": [] };
                    }
                    seriesSet[property]["data"].push([row["date"]["$date"], row[property] / (row["count"] + 1)]);
                }
            });

            // conert the "associative array" to a regular array
            series = [];
            for (var property in seriesSet) {
                if (seriesSet.hasOwnProperty(property)) {
                    series.push(seriesSet[property]);
                }
            }

            modeChartOptions.series = series;
            modeChartOptions.title.text = "Mode composition over time";

            chart = new Highcharts.Chart(modeChartOptions);
        });
    };

    $.getJSON("http://www.khanacademy.org/api/v1/exercises?callback=?", function(data) {
        var sortedExercises = data.sort(function(a, b) {
            var compA = a.display_name.toUpperCase();
            var compB = b.display_name.toUpperCase();
            return (compA < compB) ? -1 : (compA > compB) ? 1 : 0;
        });

        var option = $("<option>").text(" All ").appendTo($("#exerciselist"));
        option.attr("value", "ALL");
        $.each(sortedExercises, function(n) {
            var option = $("<option>").text(this.display_name).appendTo($("#exerciselist"));
            option.attr("value", this.name);
        });

        $("#exerciselist").val("ALL");
    });

    function populateSelections(element, prefix, names) {
        $.each(names, function(index, name) {
            var option = $("<option>").text(prefix + name).appendTo($(element));
            option.attr("value", name);
        });
    }

    populateSelections("#statlist", "", ["users", "problems", "correct", "profs", "user_exercises", "first_attempts", "hint_probs", "avg_probs_til_prof", "time_taken"]);
    populateSelections("#normlist", "", ["none", "users", "problems", "user_exercises"]);
    populateSelections("#superlist", "", ["everything", "new", "old", "coached", "uncoached", "registered", "phantom"]);
    populateSelections("#filterlist", "", ["everything"]);
    populateSelections("#filterlist", "Topic mode = ", ["true", "false", "none", "some", "majority", "all"]);
    populateSelections("#filterlist", "User type = ", ["unknown", "new", "old", "coached", "uncoached", "heavy", "light", "registered", "phantom"]);
    populateSelections("#compositionlist", "Topic mode = ", ["none", "some", "majority", "all"]);
    populateSelections("#compositionlist", "User type = ", ["unknown", "new", "old", "coached", "uncoached", "heavy", "light", "registered", "phantom"]);
    populateSelections("#formatlist", "Composition format = ", ["percentage", "count"]);

    $("#statlist").val("problems");
    $("#normlist").val("none");
    $("#superlist").val("everything");
    $("#filterlist").val("everything");
    $("#compositionlist").val("some");
    $("#formatlist").val("percentage");

    $("#exerciselist").change(function(event) {setExChart();});
    $("#statlist").change(function(event) {setExChart();});
    $("#normlist").change(function(event) {setExChart();});
    $("#superlist").change(function(event) {setExChart(); setModeChartComposition();});
    $("#filterlist").change(function(event) {setExChart(); setModeChartComposition();});
    $("#compositionlist").change(function(event) {setModeChartComparison();});
    $("#formatlist").change(function(event) {setModeChartComparison();});

    var showOrHideElements = function(show) {
        $("#superlist").toggle();
        $("#compositionlist").toggle();
        $("#formatlist").toggle();
        $("#colbreak").toggle();
        $("#mode_chart_container").toggle();
        // swap the button text
        var temp = $("#togglebutton").attr("value");
        $("#togglebutton").attr("value", $("#togglebutton").attr("data-desc"));
        $("#togglebutton").attr("data-desc", temp);
    }
    $("#togglebutton").click(showOrHideElements);
    $("#togglebutton").attr("value", BUTTON_TEXT_SIMPLIFY);
    $("#togglebutton").attr("data-desc", BUTTON_TEXT_COMPLICATE);
    // by default, show a simplified view by hiding a lot of controls
    showOrHideElements();
});
