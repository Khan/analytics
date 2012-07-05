/**
 * Common handlebar utility helpers.
 */


/**
 * Commafies large numbers.
 */
Handlebars.registerHelper("commafy", function(num) {
    return (num || 0).toString().replace(/(\d)(?=(\d{3})+$)/g, "$1,");
});

/**
 * Turns a decimal value into a whole (rounded) percent.
 */
Handlebars.registerHelper("percentify", function(num) {
    var value = Math.round(num * 100);
    return value + "%";
});

/**
 * Formats a number to a fixed number of decimal places.
 */
Handlebars.registerHelper("fixedPlaces", function(num, options) {
    return Number(num).toFixed(options.hash.digits || 0);
});
