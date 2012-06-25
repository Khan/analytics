/**
 * A simple session cache for jQuery AJAX requests to the same URL.
 */

// TODO(david): Hook onto beforeSend so ppl can continue using jQuery for ajax.
// TODO(david): Use sessionStorage? Or give the option to?
// TODO(benkomalo): Open-source this little utility. (Seems like this should
//     exist though....)

window.AjaxCache = (function() {


/**
 * Raw JSON cache of data keyed off of URLs
 */
var cache_ = {};


/**
 * A wrapper over jQuery.getJSON, which caches the results.
 */
var getJson = function getJson(url, params, callback) {
    var cacheKey = url;
    if (params) {
        cacheKey += JSON.stringify(params);
    }
    if (_.has(cache_, cacheKey)) {
        // Asynchronously call the callback to match getJSON behaviour.
        var deferred = $.Deferred();
        _.defer(function() {
            callback(cache_[cacheKey]);
            deferred.resolve();
        });
        return deferred;
    }

    var callbackProxy = function(data) {
        cache_[cacheKey] = data;
        callback(data);
    };
    return $.getJSON(url, params, callbackProxy);
};


return {
    getJson: getJson
};


})();
