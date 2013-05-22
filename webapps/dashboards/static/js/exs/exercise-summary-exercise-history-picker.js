(function() {
    /**
     * Backbone view holding page controls
     */
    var ExerciseHistoryPicker = Backbone.View.extend({
        tagName: "div",
        className: "well",
        id: "controls-container",
        template: window.ExS.Templates.summaryView,
        templateError: window.ExS.Templates.error,

        initialize: function(options) {
            _.extend(this, Backbone.Events);

            this.syncOn = options.syncOn
            this.afterRender = $.Deferred();
            this.dateDisplayFormat = "MMMM D, YYYY";
            this.dateStoreFormat = "YYYY-MM-DD";

            this.listenTo(this.syncOn, "error", this._error);
            this.listenTo(this.syncOn, "change:exercise", this._updateForm);
            this.listenTo(this.syncOn, "change:currentEndDate",
                this._updateForm);

            this.listenTo(this.syncOn, "change:currentStartDate",
                this._updateForm);

            this.listenTo(this.syncOn, "change:minDate",
                this._updateDatePickerRanges);

            this.listenTo(this.syncOn, "change:maxDate",
                this._updateDatePickerRanges);
        },

        events: {
            "typeahead:selected input[name='exercise']":
                "_updateExercise",
            "typeahead:autocompleted input[name='exercise']":
                "_updateExercise",
            "keypress input[name='exercise']": "_filterEnter",
            "date #reportrange": "_updateDate"
        },

        _error: function() {
            this._cleanup();
            $(this.templateError()).appendTo(this.$el);
            $(".alert", this.$el).alert();
        },

        _updateDatePickerRanges: function(minDate, maxDate) {
            this.afterRender.done(function() {
                $("#reportrange", this.$el).daterangepicker(
                    window.ExS.datePickerOptions(this.syncOn.get("minDate"),
                        this.syncOn.get("maxDate")));
                this._setDatePickerDisplayDate();
            });
        },

        _setDatePickerDisplayDate: function() {
            $("#reportrange input", this.$el).val(
                this.syncOn.get("currentStartDate").format(this.dateDisplayFormat) +
                " - " + this.syncOn.get(
                    "currentEndDate").format(this.dateDisplayFormat));
        },

        _updateForm: function() {
            var ttView = $("input[name='exercise']").data('ttView');
            ttView.inputView.setInputValue(window.ExS.normalizeName(
                    this.syncOn.get("exercise")));
            ttView.dropdownView.close();
            this._setDatePickerDisplayDate();
        },

        // Submit on enter in text field
        _filterEnter: function(ev) {
            if (ev.keyCode == 13) {
                this._updateExercise(ev, ev.currentTarget.value);
            }
        },

        _cleanup: function() {
            this.$el.children().remove();
        },

        // Update current exercise name to the one selected by user
        _updateExercise: function(ev, datum) {
            this.syncOn.set({
                exercise: datum.originalValue
            });
        },

        // Change current range boundaries
        _updateDate: function(ev, start, end) {
            this.syncOn.set({
                currentStartDate: start,
                currentEndDate: end
            });

            $("#reportrange input", this.$el).val(
                start.format(this.dateDisplayFormat) + " - " +
                end.format(this.dateDisplayFormat)
            );
        },

        // Render controls of the page
        render: function() {
            this._cleanup();
            $(this.template()).appendTo(this.$el);

            // Instantiate typeahead for exercise input.
            // Converts underscores and dashes in names to spaces.
            $("input[name=\"exercise\"]", this.$el).typeahead({
                name: "exercise_chosen",
                prefetch: {
                    url: "/db/exercise-summary/exercises",
                    filter: function(processedResponse) {
                        // Necessary to have nice auto completion.
                        // It's not possible to do in CSS because
                        //  javascript needs this information
                        return _.map(processedResponse.exercises, function(ex) {
                            var spacedString = window.ExS.normalizeName(ex);
                            return {
                                originalValue: ex,
                                value: spacedString,
                                tokens: spacedString.split(" ")
                            };
                        });
                    },
                    ttl: 86400000
                },
                limit: 20
            });

            // Instantiate datepicker using custom date ranges
            $("#reportrange", this.$el).daterangepicker(
                window.ExS.datePickerOptions(this.syncOn.get("minDate"),
                    this.syncOn.get("maxDate")));

            //Set the initial state of the picker label
            this._setDatePickerDisplayDate();

            this.afterRender.resolveWith(this);
            return this;
        }
    });

    _.extend(window.ExS, {
        ExerciseHistoryPicker: ExerciseHistoryPicker
    });

})();
