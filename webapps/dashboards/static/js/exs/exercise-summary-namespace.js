(function() {
    /**
     * Global functions and config for exercise summary
     */
    // Configuration for loading spinner
    var spinner = {
        lines: 13,
        length: 20,
        width: 10,
        radius: 30,
        corners: 1,
        rotate: 0,
        direction: 1,
        color: "#000",
        speed: 1,
        trail: 60,
        shadow: false,
        hwaccel: false,
        className: "spinner",
        zIndex: 2e9
    };

    var datePickerOptions = function(minDate, maxDate) {
        var ranges = {
            "Yesterday": [moment().subtract("days", 1), new Date()],
            "Last 3 Days": [moment().subtract("days", 3), new Date()],
            "Last 7 Days": [moment().subtract("days", 7), new Date()],
            "Last 14 Days": [moment().subtract("days", 14), new Date()],
            "Last 30 Days": [moment().subtract("days", 30), new Date()],
            "Last 90 Days": [moment().subtract("days", 90), new Date()],
            "Way Back": [moment("01/01/2010"), new Date()]
        };
        adjRanges = _.reduce(ranges, function(acc, range, name) {
            var newEnd = Math.min(range[1], maxDate);
            var newStart = Math.max(range[0], minDate);
            acc[name] = [newStart, newEnd];
            return acc;
        }, {});
        return {
            ranges: adjRanges,
            opens: "left",
            format: "MM/DD/YYYY",
            minDate: minDate,
            maxDate: maxDate,
            locale: {
                applyLabel: "Select"
            },
            buttonClasses: ["btn-danger"],
            dateLimit: false
        };
    };

    var fetchDefaults = {
        reset: true,
        localCache: true,
        cacheTTL: 12
    };

    var normalizeName = function(s) {
        return s.replace(/[\-\_]/g, " ");
    };

    var deferredWrapper = function(name, accept) {
        var funName = accept ? "resolveWith" : "rejectWith";
        return _.bind(function() {
            this[name][funName](this);
        }, this);
    };

    // Table generation helper
    Handlebars.registerHelper("createTable",
        function(data, order, units, id) {
            var result = "<table id='" + id +
                "' class='table table-hover table-striped'>";
            result += "<thead><tr>"

            _.each(order, function(column) {
                result += "<th>" + normalizeName(column) + "</th>";
            });

            result += "</tr></thead><tbody>"

            var dataFields = _.difference(_.keys(data[0]), order);
            _.each(data, function(dataHash) {
                result += "<tr";

                _.each(dataFields, function(dataField, idx) {
                    result += " data-" + dataField + "='" +
                        dataHash[dataField] + "'";
                });
                result += ">"

                _.each(order, function(column, idx) {
                    result += "<td>" + dataHash[column] +
                     units[idx] + "</td>";
                });
                result += "</tr>";
            });
            result += "</tbody></table>";

            return new Handlebars.SafeString(result);
    });

    Handlebars.registerHelper("noResultsHeader", function() {
        return new Handlebars.SafeString(
            "<h1 class='results-missing'>No data </h1>");
    });

    // Haskell has it and Javascript should as well!
    if ("function" !== typeof Array.prototype.sum) {
        Array.prototype.sum = function() {
            return this.reduce(function(acc, element) {
                return acc + element;
            }, 0);
        };
    }

    window.ExS = {
        spinnerOpts       : spinner,
        normalizeName     : normalizeName,
        fetchDefaults     : fetchDefaults,
        datePickerOptions : datePickerOptions,
        deferredWrapper   : deferredWrapper
    };

})();
