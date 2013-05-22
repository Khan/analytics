(function() {
    /**
     * Templates for exercise summary dashboard
     * Since some of them are reused it's best to keep them in one place
     *  this way they only have to be compiled once
     */
    window.ExS.Templates = {
        totalView       : Handlebars.compile($("#total-view").html().trim()),
        error           : Handlebars.compile(
            $("#error-template").html().trim()),
        graphDecoration : Handlebars.compile(
            $("#graph-decoration").html().trim()),
        graphProblem    : Handlebars.compile(
            $("#problem-type-graph").html().trim()),
        summaryView     : Handlebars.compile(
            $("#summary-controls").html().trim()),
        containerView   : Handlebars.compile(
            $("#container-template").html().trim())
    };
})();
