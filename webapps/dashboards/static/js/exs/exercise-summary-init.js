(function() {
    /**
     * Start the app
     * This is only in a separate file due to loading order.
     * Without some type of load time dependency resolution
     *  this has to be loaded last.
     */
    var exSumm = new window.ExS.ExerciseSummaryApp();
    exSumm.$el.appendTo("body");
    exSumm.render();

})();
