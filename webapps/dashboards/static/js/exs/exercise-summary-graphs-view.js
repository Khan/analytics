(function() {
    var GraphsView = Backbone.View.extend({
        tagName: "div",
        className: "graphs summary-graphs",
        id: "summary-graphs",
        template: window.ExS.Templates.graphDecoration,
        templateProblemType: window.ExS.Templates.graphProblem,
        templateError: window.ExS.Templates.error,

        events: {
            "click #breakdown-graphs h1": "_toggleProblemVisibility"
        },
        /**
         * Listen to Exercise collection reset event to initalize rendering
         */
        initialize: function(options) {
            _.extend(this, Backbone.Events);
            // this.on("graphs:redraw", this.onRedrawRequest);
            this.syncOn = options.syncOn;

            this.listenTo(this.syncOn, "change:exercise",
                this._onConfigurationChange);

            this.listenTo(this.syncOn, "change:currentStartDate",
                this._onConfigurationChange);

            this.listenTo(this.syncOn, "change:currentEndDate",
                this._onConfigurationChange);

            this.spinner = new Spinner(window.ExS.spinnerOpts).spin();
            this.$spinnerEl = $(this.spinner.el);
            this.$spinnerEl.css({
                top: "180px",
                left: "450px"
            });

            this.exerciseColl = new window.ExS.SubExerciseCollection({}, {});
            this.proficiencyModel = new window.ExS.ProficiencyModel();

            this.listenTo(this.exerciseColl, "error",
                this._deferredWrapper("exerciseFetch", false));

            this.listenTo(this.exerciseColl, "reset",
                this._deferredWrapper("exerciseFetch", true));

            this.listenTo(this.proficiencyModel, "error",
                this._deferredWrapper("proficiencyFetch", false));

            this.listenTo(this.proficiencyModel, "sync",
                this._deferredWrapper("proficiencyFetch", true));
        },

        _toggleProblemVisibility: function(ev) {
            $(ev.currentTarget).next().toggle(300);
        },

        _error: function() {
            this._cleanup();
            $(this.templateError()).appendTo(this.$el);
            $(".alert", this.$el).alert();
        },

        /**
         * Compute necessary values and redraw
         */
        _precomputeDataAndRedraw: function() {
            if (this.exerciseColl.length) {
               this.summaryData = this.exerciseColl.prepareSummaryGraphData();
               this.breakdownData = this.exerciseColl.prepareGraphData();
            }
            this.render();
        },

        _deferredWrapper: window.ExS.deferredWrapper,

        /**
         * Fetch and eventually draw new exercise
         */
        _onConfigurationChange: function() {
            if(this.syncOn.get("exercise")) {
                $("body").animate({ scrollTop: 180 }, "slow");

                this.exerciseColl.exercise = this.syncOn.get("exercise");
                this.proficiencyModel.set("exercise",
                    this.syncOn.get("exercise"));
                this.exerciseFetch = $.Deferred();
                this.proficiencyFetch = $.Deferred();

                // The deferreds resolveWith, rejectWith allow for overriding
                //  context of the callbacks.
                // Without "With" suffix "this"
                //  would have been
                //  [this.exerciseFetch, this.proficiencyFetch]
                // With "With" suffix "this" in these functions is
                //  [GraphsView, GraphsView] which is somewhat weird
                // Failed callback receives context
                //  of the first failed promise always
                $.when(this.exerciseFetch, this.proficiencyFetch).then(
                    function() {
                        this[0]._precomputeDataAndRedraw();
                    }, function() {
                        this._error();
                });

                this._cleanup();
                this.$spinnerEl.appendTo(this.$el);


                this.exerciseColl.fetch(_.extend(
                    _.clone(window.ExS.fetchDefaults), {
                    data: {
                        start_date: this.syncOn.get(
                            "currentStartDate").format("YYYY-MM-DD"),
                        end_date: this.syncOn.get(
                            "currentEndDate").format("YYYY-MM-DD")
                    }
                }));

                this.proficiencyModel.fetch(window.ExS.fetchDefaults);
            }
        },

        /**
         * Extract data from generated summary for display template
         */
        _gatherSummaryTemplateData: function(summaryData, profData) {
            return {
                exerciseName: window.ExS.normalizeName(summaryData.exercise),
                originalExerciseName: summaryData.exercise,
                proficient: (profData.earned_proficiency * 100 /
                    profData.total_users).toFixed(1),

                averageTime: Math.round(summaryData.timeTaken /
                    summaryData.total),

                totalAttempts: d3.format(".3s")(summaryData.total)
            };
        },

        /**
         * Extract data from generated summary for display template
         */
        _gatherProblemTypeTemplateData: function(problemTypeData) {
            return {
                problemName: problemTypeData.problemType,
                averageTime: Math.round(problemTypeData.timeTaken /
                    problemTypeData.total),

                totalAttempts: d3.format(".3s")(problemTypeData.total)
            };
        },

        _cleanup: function() {
            $("svg .arc, svg .prob-rect, svg .bar, #prof-number", this.$el)
                .popover("destroy");
            this.$el.children().remove();
        },

        /**
         * Render per problem type template and append it to
         * appendElem. Used in conjunction with iterator
         */
        _drawProblemType: function(appendElem) {
            return _.bind(function(dataSeries) {
                var generatedTemplate = this.templateProblemType(
                    this._gatherProblemTypeTemplateData(dataSeries)
                );
                $(generatedTemplate.trim()).appendTo(appendElem);

                this._drawExercisePieChart(dataSeries.series,
                    "#problem-type-graph-" + dataSeries.problemType, 225, 260);

                this._drawExerciseBarChart(dataSeries.series,
                    "#problem-attempts-graph-" + dataSeries.problemType);
            }, this);
        },

        /**
         * Extremely messy render.
         * Mostly due to bootstrap popover and fact
         *  that D3 can't render object detached from document.
         */
        render: function() {
            this._cleanup();
            if (this.exerciseColl.length === 0) {
                $(this.template({empty: true}).trim()).appendTo(this.$el);
            } else {
                // Draw dashboard DOM
                $(this.template(this._gatherSummaryTemplateData(
                    this.summaryData, this.proficiencyModel.attributes)
                ).trim()).appendTo(this.$el);

                // Draw main pie chart and append it to DOM
                this._drawExercisePieChart(
                    this.summaryData.series, "#total-graph", 460, 540);

                // Draw small bar chart statistics
                this._drawExerciseBarChart(
                    this.summaryData.series, "#attempts-graph");

                // Draw large graph comparing different problem types
                this._drawBreakdownChart(this.breakdownData.slice(0, 6),
                    "#breakdown-graphs");

                // Draw all of the problem types graphs
                _.each(this.breakdownData, this._drawProblemType(
                    $("#breakdown-graphs", this.$el)));

                $("#breakdown-graphs h1", this.$el).next().hide();

                $("#prof-number").popover({
                    title: "Note",
                    content: ["<p class=\"notice\">",
                        "This number is independent of date range ",
                        "selected. Includes users who had their first ",
                        "attempt at the exercise in last 6 months",
                        "</p>"].join(""),
                    trigger: "hover",
                    html: true,
                    container: "body",
                    delay: { show: 200, hide: 300 }
                });

                $("svg .arc, svg .prob-rect, svg .bar", this.$el).popover({
                    title: function() {
                        var d = this.__data__;
                        if (d.data) {
                            d = d.data;
                        }
                        return [d.name, "answers"].join(" ");
                    },
                    content: function() {
                        var d = this.__data__;
                        var suff = "";
                        if (d.data) {
                            d = d.data;
                            suff = "%";
                        }
                        return d.attempts + suff;
                    },
                    trigger: "hover",
                    html: true,
                    container: "body",
                    delay: { show: 200, hide: 300 }
                });
            }
        },

        /**
         * Draw pie chart showing correct and wrong percentages
         */
        _drawExercisePieChart: function(dataSeries, elemId, width, height) {
            var radius = Math.min(width, height) / 2;

            // Sets up dimension of the pie chart
            var arc = d3.svg.arc()
                .outerRadius(radius - 10)
                .innerRadius(radius > 200 ? radius - 80 : radius - 60);

            var pie = d3.layout.pie()
                .sort(null)
                .value(function(d) { return d.attempts; });

            // Create main SVG element
            var svg = d3.select(elemId).append("svg")
                .attr("width", width)
                .attr("height", height)
              .append("g")
                .attr("transform", "translate(" + width / 2 + "," +
                 height / 2 + ")");

            // Transform total number into percentages
            var total = _.pluck(dataSeries, "attempts").sum();

            var percentSummary = _.map(dataSeries, function(series) {
                return {
                    attempts: (series.attempts * 100 / total).toFixed(2),
                    name: series.name
                };
            });

            // Draw arcs of doughnut chart
            var g = svg.selectAll(".arc")
                .data(pie(percentSummary))
                .enter().append("g")
                .attr("class", "arc");

            // Create path along which text will be positioned on the chart
            g.append("path")
                .attr("d", arc)
                .attr("class", function(d) { return d.data.name });

            // Attach text to path which serve as series description
            g.append("text")
                .attr("transform", function(d) { return "translate(" +
                 arc.centroid(d) + ")"; })
                .attr("dy", "8px")
                .attr("class", function(d) {
                    return radius > 180 ? "pie-text large" : "pie-text";
                })
                .style("text-anchor", "middle")
                .text(function(d) { return d.data.name; });
        },

        /**
         * Draw bar chart showing number of attempts instead of percentages
         */
        _drawExerciseBarChart: function(dataSeries, elemId) {
            var margin = {top: 20, right: 20, bottom: 30, left: 40},
                width = 200 - margin.left - margin.right,
                height = 160 - margin.top - margin.bottom;

            // Scale max domain by small amount to have y axis
            //  extend beyond chart bars
            var maxDomainValue = d3.max(dataSeries, function(d) {
                return d.attempts; }) * 1.1;
            var format = d3.format(".2s");

            // Creates a function which allows us to map from domain
            // (defined later) to values of this axis
            var x = d3.scale.ordinal()
                .rangeRoundBands([0, width], .1);

            // Linear scale, nice() will make all tick values integer
            var y = d3.scale.linear()
                .nice()
                .range([height, 0]);

            var xAxis = d3.svg.axis()
                .scale(x)
                .orient("bottom");

            // Draw y-axis, with "nice" tick values
            var yAxis = d3.svg.axis()
                .scale(y)
                .tickValues(_.map(y.ticks(4), function(t) {
                    return t * maxDomainValue }))
                .orient("left")
                .tickFormat(function(d) {
                    return format(Math.round(d));
                });

            // Create main SVG element
            var svg = d3.select(elemId).append("svg")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom)
              .append("g")
                .attr("transform", "translate(" + margin.left + "," +
                 margin.top + ")");

            x.domain(_.map(dataSeries, function(d) { return d.name; }));
            y.domain([0, maxDomainValue]);

            // Draw x-axis as defined by xAxis
            svg.append("g")
              .attr("class", "x axis")
              .attr("transform", "translate(0," + height + ")")
              .call(xAxis);

            // Draw y-axis as defined by yAxis
            svg.append("g")
              .attr("class", "y axis")
              .call(yAxis);

            // Append grid to the graph
            // Trick is to draw it using ticks
            //  as long as graph and remove labels
            svg.append("g")
                .attr("class", "grid")
                .call(yAxis.ticks(4)
                    .tickSize(-width, 0, 0)
                    .tickFormat(""));

            // Draw bars of the bar chart
            svg.selectAll(".bar")
                .data(dataSeries)
            .enter().append("rect")
              .attr("class", "bar")
              .attr("x", function(d) { return x(d.name); })
              .attr("width", x.rangeBand())
              .attr("y", function(d) { return y(d.attempts); })
              .attr("height", function(d) { return height - y(d.attempts); });

        },

        /**
         * Draws groupped bar chart allowing to compare
         *  problem types in exercise
         */
        _drawBreakdownChart: function(dataSeries, elemId) {
            var margin = {top: 20, right: 20, bottom: 30, left: 40},
                width = 940 - margin.left - margin.right,
                height = 500 - margin.top - margin.bottom;

            // Create main scale
            var x0 = d3.scale.ordinal()
                .rangeRoundBands([0, width], .1);

            // Create scale for different data series for given domain value
            var x1 = d3.scale.ordinal();

            var y = d3.scale.linear()
                .range([height, 0]);

            var xAxis = d3.svg.axis()
                .scale(x0)
                .orient("bottom");

            var yAxis = d3.svg.axis()
                .scale(y)
                .orient("left")
                .tickFormat(d3.format(".2s"));

            // Create main SVG element
            var svg = d3.select(elemId).append("svg")
                .attr("class", "comparison-graph")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom)
              .append("g")
                .attr("transform", "translate(" + margin.left + "," +
                 margin.top + ")");

            // Extract data series names
            var domainNames = _.map(dataSeries[0].series, function(d) {
                return d.name;
            });

            // x0 domain are problem types
            x0.domain(_.map(dataSeries, function(d) { return d.problemType; }));
            // x1 domain are data series which we plot
            x1.domain(domainNames).rangeRoundBands([0, x0.rangeBand()]);
            y.domain([0, d3.max(dataSeries, function(d) {
                return d3.max(d.series, function(d) { return d.attempts; });
            })]);

            // Create markup with attached data to which bars will be attaced
            var prob = svg.selectAll(".problem")
              .data(dataSeries)
            .enter().append("g")
              .attr("class", "g")
              .attr("transform", function(d) { return "translate(" +
                x0(d.problemType) + ",0)"; });

            // Draw bars using specified domains and previously
            //  created elements
            prob.selectAll("rect")
              .data(function(d) { return d.series; })
            .enter().append("rect")
              .attr("class", function(d) { return "prob-rect " + d.name })
              .attr("width", x1.rangeBand())
              .attr("x", function(d) { return x1(d.name); })
              .attr("y", function(d) { return y(d.attempts); })
              .attr("height", function(d) { return height - y(d.attempts); });

            // Create x Axis
            svg.append("g")
              .attr("class", "x axis")
              .attr("transform", "translate(0," + height + ")")
              .call(xAxis);

            // Create y Axis with label
            svg.append("g")
              .attr("class", "y axis")
              .call(yAxis)
            .append("text")
              .attr("transform", "rotate(-90)")
              .attr("y", 6)
              .attr("dy", "16px")
              .style("text-anchor", "end")
              .text("Attempts");

            // Draw legend in upper right corner
            var legend = svg.selectAll(".legend")
              .data(domainNames.slice().reverse())
            .enter().append("g")
              .attr("class", "legend")
              .attr("transform", function(d, i) { return "translate(0," +
               i * 20 + ")"; });

            // Draw colour rectangles
            legend.append("rect")
              .attr("x", width - 18)
              .attr("width", 18)
              .attr("height", 18)
              .attr("class", function(d) { return d; });

            // Add description to colours
            legend.append("text")
              .attr("x", width - 24)
              .attr("y", 9)
              .attr("dy", "8px")
              .style("text-anchor", "end")
              .text(function(d) { return d; });
        }

    });

    _.extend(window.ExS, {
        GraphsView: GraphsView
    });

})();
