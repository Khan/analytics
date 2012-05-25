/**
 * Logic for fetching data and rendering the daily video
 * statistics dashboard.
 */

// Namespace
var VideoStats = {};

/**
 * Entry point - called on DOMReady event.
 */
VideoStats.init = function() {
    // TODO(benkomalo): consolidate this with the server info in
    // daily-ex-stats.js (maybe abstract to a data fetcher)
    var BASE_STAT_SERVER_URL = "http://184.73.72.110:27080/";

    var url = BASE_STAT_SERVER_URL +
         "report/daily_video_stats/_find?callback=?";
    var datestamp = $("#datestamp").val(); 
    var user_category = $("#user_category").val(); 
    var criteria = '{"date_str":"' + datestamp + '","ucat":"' 
        + user_category+ '"}';
    var params = {
        //json query 
        "criteria": criteria,
        "batch_size": 15000
    };
    $.getJSON(url, params, VideoStats.handleDataLoad);
};
VideoStats.addCommas = function(nStr) {
	nStr += '';
	var x = nStr.split('.');
	var x1 = x[0];
	var x2 = x.length > 1 ? '.' + x[1] : '';
	var rgx = /(\d+)(\d{3})/;
	while (rgx.test(x1)) {
		x1 = x1.replace(rgx, '$1' + ',' + '$2');
	}
	return x1 + x2;
};


/**
 * Handles the raw JSON data returned from the server.
 * @param {Object} data The raw data with fields including:
 *     rows - JSON object for each video record summary
 *     total_rows - total length of the rows
 *     query - JSON object representing the original query
 */
VideoStats.handleDataLoad = function(data) {
    if (!data["results"]) {
        // TODO(benkomalo): handle - data is empty.
    }
    var results = data["results"];
    var i;
    for (i = 0; i < results.length; i+=1) { 
        if (results[i]['vid'] === 'total') {
            results[i]["link"] = "<b>Total</b>"
        } else {
            results[i]["link"] = '<a href="http://youtube.com/watch?v=' + 
                results[i]["vid"] + '">' + results[i]["vtitle"] + '</a>';
        }             
        results[i]["hours_watched"] = VideoStats.addCommas(
           Math.floor(results[i]["seconds_watched"]/3600));
        results[i]["watched"] = 
            VideoStats.addCommas(results[i]["watched"]);
        results[i]["completed"] = 
            VideoStats.addCommas(results[i]["completed"]);
    } 
    
    VideoStats.renderVideosTable(results);
};

// TODO(benkomalo): have a configurable sort
/**
 * Renders the main stats table. Each row is a record summarizing the stats on
 * a video level (e.g. how many people completed it).
 */
VideoStats.renderVideosTable = function(jsonRows) {
    var tableTemplate = Handlebars.compile($("#video-table").text());
    var table = $(tableTemplate());

    var rowTemplate = Handlebars.compile($("#video-row-template").text());
    _.chain(jsonRows)
        .sortBy(function(row) { return -row["seconds_watched"]; })
        .each(function(row) { $(rowTemplate(row)).appendTo(table) });

    var container = $("#video-stats-container");
    container.html("");
    container.append(table);
};

$(document).ready(function() {
    $("#datestamp").change(function(event) { VideoStats.init();});
    $("#user_category").change(function(event) { VideoStats.init();});
    VideoStats.init();
});

