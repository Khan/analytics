(function() {
    // View binding controls and graphs together
    // Passes messages between the two
    var ExerciseSummaryApp = Backbone.View.extend({
        tagName   : "div",
        className : "container summary-container",
        id        : "summary-container",
        template  : window.ExS.Templates.containerView,

        // Carousel events: "slide" is triggered on effect initialization
        //      and "slid" is triggered on finish
        events: {
            "slide .carousel": "_hideNavigation",
            "slid .carousel": "_showNavigation"
        },

        _showNavigation: function(ev) {
            var navButton = $(".carousel-control", this.$el);
            navButton.fadeIn("fast");
        },

        // Moves back to correct position after showing graphs
        // Switches button sides
        _hideNavigation: function(ev) {
            var $body = $("body");
            var navButton = $(".carousel-control", this.$el);
            if(this.lastPos !== $body.scrollTop()) {
                var newPos = navButton.text() === "›" ? 180 : this.lastPos;
                $body.animate({ scrollTop: newPos }, "slow");
            }
            this.lastPos = $body.scrollTop();
            navButton.fadeOut(function() {
                navButton.toggleClass("left");
                navButton.toggleClass("right");
                navButton.data("slide",
                    navButton.data("slide") === "next" ? "prev" : "next");
                var contents = navButton.text();
                navButton.text(contents === "›" ? "‹" : "›");
            });
        },

        initialize: function() {
            _.extend(this, Backbone.Events);
            this.state =
                new window.ExS.CurrentParametersModel();
            this.controls = new window.ExS.ExerciseHistoryPicker({syncOn: this.state});
            this.graphs = new window.ExS.GraphsView({syncOn: this.state});
            this.totals = new window.ExS.SummaryTableView({syncOn: this.state});
            this.listenTo(this.totals, "click:row", _.bind(function() {
                $(".carousel", this.$el).carousel("next");
            }, this));

            $(window).on("hashchange", _.bind(this._restoreUsingHash, this));
        },

        // Provides notion of state to dashboard.
        // User coming back will be presented appropriate exercise
        _restoreUsingHash: function() {
            hash = window.location.hash;
            if(hash) {
                this.state.set({"exercise": hash.slice(1)});
                $(".carousel", this.$el).carousel(0);
            }
        },

        render: function() {
            $(this.template()).appendTo(this.$el);
            this.controls.$el.appendTo($(".item", this.$el).first());
            this.totals.$el.appendTo($(".item", this.$el).last());
            this.graphs.$el.appendTo($(".item", this.$el).first());
            this.controls.render();

            $(".carousel", this.$el).carousel({
                // Don't auto rotate the items
                interval: false
            });
            this._restoreUsingHash();
            return this;
        }
    });

    _.extend(window.ExS, {
        ExerciseSummaryApp: ExerciseSummaryApp
    });

})();
