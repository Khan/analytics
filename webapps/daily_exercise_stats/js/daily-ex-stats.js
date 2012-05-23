/**
 * Logic for fetching data and rendering the daily exercise
 * statistics dashboard.
 */

var exChartOptions = {
    chart: {
        renderTo: 'ex_chart_container',
        defaultSeriesType: 'spline'
    },
    title: {
        text:'Exercise over time'
    },
    xAxis: {
        type: "datetime",
        dateTimeLabelFormats: { day: "%a %e %b" }
    },
    yAxis: {
        title: { text:'' }
    },
    series: []
};
var modeChartOptions = {
    chart: {
        renderTo: 'mode_chart_container',
        defaultSeriesType: 'spline'
    },
    title: {
        text:'Mode stats over time'
    },
    xAxis: {
        type: "datetime",
        dateTimeLabelFormats: { day: "%a %e %b" }
    },
    yAxis: {
        title: { text:'' }
    },
    series: []
};

var BUTTON_TEXT_COMPLICATE = "I'm not nearly confused enough.  Give me more power!";
var BUTTON_TEXT_SIMPLIFY = "Too. Much. Power.  Make it simpler!"

jQuery( document ).ready(function() {

    var chart;

    var setExChart = function() {

        var ex_name = jQuery( "#exerciselist" ).val();
        var stat_name = jQuery( "#statlist" ).val();
        var norm_name = jQuery( "#normlist" ).val();
        var super_mode =  jQuery( "#superlist" ).val();
        var tmodes = jQuery( "#filterlist" ).val();
        var stat_desc = stat_name + ((norm_name=="none") ? "" : (" divided by " + norm_name));

        // load all data from the collection
        var series = {};

        var chartSeries = [],
            chartTitles = [];

        if (!tmodes instanceof Array) {
            tmodes = [tmodes];
        }

        var deferreds = [];
        $.each(tmodes, function(ix, tmode) {

            var url = "http://184.73.72.110:28017/report/daily_ex_stats/?filter_exercise=" + ex_name + "&filter_filter_mode=" + tmode + "&filter_super_mode=" + super_mode + "&jsonp=?";
            var seriesName = stat_name + " (filter mode: " + tmode + ")";

            deferreds[deferreds.length] = jQuery.getJSON(url, function( sdata ) {

                series = sdata;

                // this function takes the result of the mongodb simple REST api
                // and extracts data in the format expected by highcharts, specifically
                // [[date1,val1], [date2, val2], ...]
                var get_data = function (series, date_field) {
                    var data = [];
                    var rows = series["rows"];
                    jQuery.each(rows, function(ex_name, row) {
                        var denom = 1.0;
                        if (norm_name != "none" && norm_name in row) {
                            denom = row[norm_name];
                        };
                        if (date_field in row && stat_name in row) {
                            data.push([ row[date_field]["$date"], row[stat_name]/denom ]);
                        };
                    });
                    return data;
                };

                chartSeries[chartSeries.length] = { name: seriesName, data: get_data(series, "date", stat_name) };
                chartTitles[chartTitles.length] = ex_name + ": " + stat_desc + " over time";

            });

        });

        $.when.apply(null, deferreds).done(function() {

            exChartOptions.series = chartSeries;
            exChartOptions.title.text = chartTitles[0];

            chart = new Highcharts.Chart( exChartOptions );

        });

    };

    // this version of the mode chart shows the prevelance of a certain submode within the list of selected filters
    var setModeChartComparison = function() {

        if (! $("#mode_chart_container").is(":visible")) {
            return;
        }

        var super_mode =  jQuery( "#superlist" ).val();
        var tmodes = jQuery( "#filterlist" ).val();
        if (!tmodes instanceof Array) {
            tmodes = [tmodes];
        }
        var sub_mode = jQuery( "#compositionlist" ).val();
        var use_percentages = (jQuery( "#formatlist" ).val()=="percentage");

        // load all data from the collection
        var series = {};

        var chartSeries = [],
            chartTitles = [];

        var deferreds = [];
        $.each(tmodes, function(ix, tmode) {

            var url = "http://184.73.72.110:28017/report/daily_ex_mode_stats/?filter_filter_mode=" + tmode + "&filter_super_mode=" + super_mode + "&jsonp=?";

            deferreds[deferreds.length] = jQuery.getJSON(url, function( sdata ) {

                series = sdata;
                console.log(series);
                // this function takes the result of the mongodb simple REST api
                // and extracts data in the format expected by highcharts, specifically
                // [[date1,val1], [date2, val2], ...]
                var get_data = function (series, date_field) {
                    var data = [];
                    var rows = series["rows"];
                    jQuery.each(rows, function(ex_name, row) {
                        var denom = 1.0;
                        if (use_percentages) {
                            denom = row["count"] + 0.1;
                        };
                        if (date_field in row && sub_mode in row) {
                            data.push([ row[date_field]["$date"], row[sub_mode]/denom ]);
                        };
                    });
                    return data;
                };

                chartSeries[chartSeries.length] = { name: sub_mode + " (" + tmode + ")", data: get_data(series, "date", sub_mode) };
                chartTitles[chartTitles.length] = "Prevalance of " + sub_mode + " in selected segments over time";

            });

        });

        $.when.apply(null, deferreds).done(function() {

            modeChartOptions.series = chartSeries;
            modeChartOptions.title.text = chartTitles[0];

            chart = new Highcharts.Chart( modeChartOptions );

        });


    };

    // this version of the mode chart shows the complete submode composition within the top selected filter
    var setModeChartComposition = function() {

        if (! $("#mode_chart_container").is(":visible")) {
            return;
        }

        var super_mode =  jQuery( "#superlist" ).val();
        var tmode = jQuery( "#filterlist" ).val();
        if (tmode instanceof Array && tmode.length > 0) {
            tmode = tmode[0];
        }

        var url = "http://184.73.72.110:28017/report/daily_ex_mode_stats/?filter_filter_mode=" + tmode + "&filter_super_mode=" + super_mode + "&jsonp=?";

        jQuery.getJSON(url, function( sdata ) {

            var series_set = Array();
            var query_rows = sdata["rows"];
            jQuery.each(query_rows, function(ex_name, row) {
                for (var property in row) {
                    if (!row.hasOwnProperty(property)) { continue; }
                    //TODO(jace): 'some' must be a reserved word
                    if (property in {'_id':'', 'count':'', 'filter_name':'', 'date':'', 'some':''} ) { continue; }
                    if (!(property in series_set)) {
                        series_set[property] = { 'name': property, 'data': [] };
                    };
                    series_set[property]['data'].push( [ row['date']['$date'], row[property]/(row['count']+1) ] );
                };
            });

            // conert the "associative array" to a regular array
            series = [];
            for (var property in series_set) {
                if (series_set.hasOwnProperty(property)) {
                    series.push( series_set[property] );
                }
            }

            modeChartOptions.series = series;
            modeChartOptions.title.text = "Mode composition over time";

            chart = new Highcharts.Chart( modeChartOptions );

        });


    };

    jQuery.getJSON( "http://www.khanacademy.org/api/v1/exercises?callback=?", function( data ) {
        var sortedExercises = data.sort( function(a, b) {
            var compA = a.display_name.toUpperCase();
            var compB = b.display_name.toUpperCase();
            return (compA < compB) ? -1 : (compA > compB) ? 1 : 0;
        });

        var option = jQuery( "<option>" ).text( " All " ).appendTo( jQuery( "#exerciselist" ) );
        option.attr( "value", "ALL" );
        jQuery.each( sortedExercises, function( n ) {
            var option = jQuery( "<option>" ).text( this.display_name ).appendTo( jQuery( "#exerciselist" ) );
            option.attr( "value", this.name );
        });

        jQuery( "#exerciselist" ).val("ALL");

    });

    function populate_selections(element, prefix, names) {
        jQuery.each( names, function (index, name) {
            var option = jQuery( "<option>" ).text(prefix+name).appendTo( jQuery( element ) );
            option.attr( "value", name );
        });
    }

    populate_selections("#statlist", "", ['users', 'problems', 'correct', 'profs', 'user_exercises', 'first_attempts', 'hint_probs', 'avg_probs_til_prof', 'time_taken']);
    populate_selections("#normlist", "", ['none', 'users', 'problems', 'user_exercises']);
    populate_selections("#superlist", "", ['everything', 'new', 'old', 'coached', 'uncoached', 'registered', 'phantom']);
    populate_selections("#filterlist", "", ['everything']);
    populate_selections("#filterlist", "Topic mode = ", ['true', 'false', 'none', 'some', 'majority', 'all']);
    populate_selections("#filterlist", "User type = ", ['unknown', 'new', 'old', 'coached', 'uncoached', 'heavy', 'light', 'registered', 'phantom']);
    populate_selections("#compositionlist", "Topic mode = ", ['none', 'some', 'majority', 'all']);
    populate_selections("#compositionlist", "User type = ", ['unknown', 'new', 'old', 'coached', 'uncoached', 'heavy', 'light', 'registered', 'phantom']);
    populate_selections("#formatlist", "Composition format = ", ['percentage', 'count']);

    jQuery( "#statlist" ).val("problems");
    jQuery( "#normlist" ).val("none");
    jQuery( "#superlist" ).val("everything");
    jQuery( "#filterlist" ).val("everything");
    jQuery( "#compositionlist" ).val("some");
    jQuery( "#formatlist" ).val("percentage");

    jQuery( "#exerciselist" ).change(function( event ) {setExChart();});
    jQuery( "#statlist" ).change(function( event ) {setExChart();});
    jQuery( "#normlist" ).change(function( event ) {setExChart();});
    jQuery( "#superlist" ).change(function( event ) {setExChart(); setModeChartComposition();});
    jQuery( "#filterlist" ).change(function( event ) {setExChart(); setModeChartComposition();});
    jQuery( "#compositionlist" ).change(function( event ) {setModeChartComparison();});
    jQuery( "#formatlist" ).change(function( event ) {setModeChartComparison();});

    var show_or_hide_elements = function (show) {
        $('#superlist').toggle();
        $('#compositionlist').toggle();
        $('#formatlist').toggle();
        $('#colbreak').toggle();
        $('#mode_chart_container').toggle();
        // swap the button text
        var temp = jQuery('#togglebutton').attr('value');
        $('#togglebutton').attr( 'value', $('#togglebutton').attr('data-desc') );
        $('#togglebutton').attr('data-desc', temp);
    }
    jQuery( "#togglebutton" ).click( show_or_hide_elements );
    jQuery( "#togglebutton" ).attr('value', BUTTON_TEXT_SIMPLIFY);
    jQuery( "#togglebutton" ).attr('data-desc', BUTTON_TEXT_COMPLICATE);
    // by default, show a simplified view by hiding a lot of controls
    show_or_hide_elements();

});
