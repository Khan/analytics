/**
 * Dashboard for teacher and student growth metrics.
 *
 * Displays charts showing time series of total number of teachers and students
 *  as well as number of active users from these categories
 * At the end is the map that holds locations of the teachers using our site.
 * Makes use of Marker Clusterer (from Google) and OverlappingMarkerSpiderfier
 * (https://github.com/jawj/OverlappingMarkerSpiderfier, it's called like this
 *  because it creates spiders on the map)
 *  to deal with overcrowding of markers.
 */

(function() {

    var infoWindowTpl = Handlebars.compile(
        $("#info-window-tpl").html().trim());

    var parseMongoResponse = function(model) {
        return function(response) {
            return _.map(response.results, function(dataPoint) {
                return new model(dataPoint);
            });
        };
    }

    // Handler for marker click
    var markerClickHandler = function(marker, e) {
        if(googleMap.infoWindow.getMap() &&
            googleMap.infoWindow.getPosition() === marker.position) {
                googleMap.infoWindow.close();
        } else {
            googleMap.infoWindow.close();
            e.cancelBubble = true;
            e.returnValue = false;
            if (e.stopPropagation) {
              e.stopPropagation();
              e.preventDefault();
            }

            googleMap.infoWindow.setContent(infoWindowTpl(marker.record));
            googleMap.infoWindow.setPosition(marker.position);
            googleMap.infoWindow.open(googleMap.map);
        }
    };

    var closeInfoWindow = function() {
        googleMap.infoWindow.close();
    };

    // Maps specific setup.
    // This reference is really helpful:
    //  https://developers.google.com/maps/documentation/javascript/reference
    var googleMap = {};
    googleMap.infoWindow = new google.maps.InfoWindow();
    googleMap.shadow = new google.maps.MarkerImage(
        'https://www.google.com/intl/en_ALL/mapfiles/shadow50.png',
        new google.maps.Size(37, 34), // size - for sprite clipping
        new google.maps.Point(0, 0), // origin - ditto
        new google.maps.Point(10, 34) // anchor - where to meet map location
    );

    googleMap.map = new google.maps.Map(document.getElementById('geo-teachers'), {
        zoom: 3,
        maxZoom: 16,
        center: new google.maps.LatLng(30, -55),
        mapTypeId: google.maps.MapTypeId.ROADMAP
    });

    // Handles the case when there are multiple markers on the exact same spot
    googleMap.oms = new OverlappingMarkerSpiderfier(googleMap.map,
        {markersWontMove: true, markersWontHide: true, keepSpiderfied: true});

    // Does marker clustering so the map is not overcrowded
    googleMap.markerClusterer = new MarkerClusterer(googleMap.map, [], {
        maxZoom: 15,
        gridSize: 40
    });

    googleMap.oms.addListener('click', markerClickHandler);

    // Triggered when multiple points are moved aside (due to overlapping)
    //  to view them
    googleMap.oms.addListener('spiderfy', closeInfoWindow);

    google.maps.event.addListener(googleMap.map, 'click', closeInfoWindow);

    google.maps.event.addListener(googleMap.map, 'idle', function() {
        // When user pans or zooms the map update the markers
        googleMap.oms.clearMarkers();
        googleMap.markerClusterer.clearMarkers();
        handleMarkers(markerCollection);
    });

    // Database URLs
    var BASE_DB_URL = "http://107.21.23.204:27080/report/";
    var COUNT_URL = "student_teacher_count/_find?callback=";
    var GEO_URL = "teacher_country/_find?callback=";
    var COUNT_QUERY_URL = BASE_DB_URL + COUNT_URL;
    var GEO_QUERY_URL = BASE_DB_URL + GEO_URL;
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

        parse: parseMongoResponse(CountEntryModel),

        url: COUNT_QUERY_URL + "?&batch_size=15000"
    });

    // Model for geolocation data
    var GeoTeacherModel = Backbone.Model.extend({
        defaults: {
            city: "",
            user_id: "",
            ip: "0.0.0.0",
            region: "",
            joined: 0,
            longitude: 0,
            teacher: "",
            country_code: "",
            country: "",
            latitude: 0,
            user_nickname: "",
            user_email: ""
        },
        cid: "teacher",

        initialize: function(attr, options) {
            Backbone.Model.prototype.initialize.call(this, attr, options);
            // Unfortunately this case is not handled by handlebars if
            if(attr.user_nickname === "null") {
                this.set({
                    user_nickname: attr.user_email
                });
            }
        },

        parse: function(response) {
            return response.results;
        },

        url: ""
    });

    // Collection keeping the geolocation models
    var GeoTeacherCollection = Backbone.Collection.extend({
        model: GeoTeacherModel,

        parse: parseMongoResponse(GeoTeacherModel),

        url: GEO_QUERY_URL + "?&batch_size=15000"
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

    // Draw markers
    // Only draw first 4000 markers that are located in the current viewport
    var handleMarkers = function(data) {
        onReady.done(function() {
            var mapBounds = googleMap.map.getBounds();
            var markers = _.chain(data.models)
            .pluck("attributes")
            .filter(function(record) {
                return mapBounds.contains(new google.maps.LatLng(
                    record.latitude, record.longitude));
            })
            .first(4000)
            .map(function(record) {
                var latLng = new google.maps.LatLng(record.latitude,
                    record.longitude);

                var marker = new google.maps.Marker({
                    position: latLng,
                    shadow: googleMap.shadow
                });

                marker.record = record;
                // There's no API to add more than one marker at once
                googleMap.oms.addMarker(marker);
                return marker;
            }).value();

            googleMap.markerClusterer.addMarkers(markers);
        });
    };

    // Kick everything off by creating and populating collection
    var dataCollection = new CountSeriesCollection();
    var markerCollection = new GeoTeacherCollection();

    dataCollection.on("reset", handleGrowthData);
    markerCollection.on("reset", handleMarkers);

    dataCollection.fetch({
        cache: true,
        reset: true,
        ttl: 12
    });

    markerCollection.fetch({
        cache: true,
        reset: true,
        ttl: 12
    });

    $(function() {
        // Draw graphs when DOM is loaded
        onReady.resolve();
    });

})();
