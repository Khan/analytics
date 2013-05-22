(function() {
    /**
     * View representing table showing all exercise data
     */
    var SummaryTableView = Backbone.View.extend({
        tagName: "div",
        id: "summary-tables",
        className: "summary-tables",
        template: window.ExS.Templates.totalView,
        templateTable: window.ExS.Templates.totalTable,
        templateError: window.ExS.Templates.error,
        templateProf: window.ExS.Templates.profTable,
        dataTableConfig: {
            "sDom": ["<'row'<'span6'l><'span6'f>r>t",
                    "<'row'<'span6'i><'span6'p>>"].join(""),
            "sPaginationType": "bootstrap",
            "oLanguage": {
                "sLengthMenu": "_MENU_ records per page"
            },
            "aaSorting": [[1,'desc']]
        },
        requestDateFormat: "YYYY-MM-DD",
        displayDateFormat: "MMMM D, YYYY",

        _attemptFetchDefaults: function() {
            return _.extend(_.clone(window.ExS.fetchDefaults), {
                data: {
                    start_date: this.syncOn.get("currentStartDate")
                        .format(this.requestDateFormat),
                    end_date: this.syncOn.get("currentEndDate")
                        .format(this.requestDateFormat)
                }
            });
        },

        events: {
            "click tbody tr": "_onTableRowClick",
            "date #summaryrange": "_onDatePickerChange"
        },

        // Set up initial date ranges
        // Intialize underlying model
        // Listen to change events on models
        // Kick everything off by dowloading first set of data
        initialize: function(options) {
            _.extend(this, Backbone.Events);
            this.attemptColl = new window.ExS.Exercises();
            this.proficiencyColl = new window.ExS.Proficiencies();
            this.attemptFetch = $.Deferred();
            this.profFetch = $.Deferred();
            this.afterRender = $.Deferred();
            this.syncOn = options.syncOn;

            this.listenTo(this.attemptColl, "reset",
                this._deferredWrapper("attemptFetch", true));

            this.listenTo(this.attemptColl, "error",
                this._deferredWrapper("attemptFetch", false));

            this.listenTo(this.proficiencyColl, "reset",
                this._deferredWrapper("profFetch", true));

            this.listenTo(this.proficiencyColl, "error",
                this._deferredWrapper("profFetch", false));

            // We need to synchronise initial load
            $.when(this.attemptFetch, this.profFetch)
                .then(function() {
                    this[0].render();
                }, function() {
                    this._error();
            });

            this.listenTo(this.syncOn, "error", this._error);
            this.listenTo(this.syncOn, "change:currentEndDate",
                this.fetchAttempts);

            this.listenTo(this.syncOn, "change:currentStartDate",
                this.fetchAttempts);

            this.listenTo(this.syncOn, "change:minDate",
                this.updateDatePickerRanges);

            this.listenTo(this.syncOn, "change:maxDate",
                this.updateDatePickerRanges);

            this.spinner = new Spinner(window.ExS.spinnerOpts).spin();
            this.$spinnerEl = $(this.spinner.el);
            this.$spinnerEl.css({
                top: "180px",
                left: "450px"
            });
            this.$spinnerEl.appendTo(this.$el);

            this.fetchAttempts();

            this.proficiencyColl.fetch(window.ExS.fetchDefaults);
        },

        _cleanup: function() {
            this.$el.children().remove();
        },

        _error: function() {
            this._cleanup();
            $(this.templateError()).appendTo(this.$el);
            $(".alert", this.$el).alert();
        },

        // Show detailed report for exercise - "graph" view
        _onTableRowClick: function(ev) {
            this.syncOn.set({
                exercise:  $(ev.currentTarget).data("originalname")
            });
            this.trigger("click:row");
        },

        // Fetch data set for different date
        _onDatePickerChange: function(ev, start, end) {
            this._cleanup();
            this.$spinnerEl.appendTo(this.$el);
            this.attemptFetch = $.Deferred();
            this.attemptFetch.done(function() {
                this.render();
            });

            this.syncOn.set({
                currentStartDate: start,
                currentEndDate: end
            });
        },

        _deferredWrapper: window.ExS.deferredWrapper,

        fetchAttempts: function() {
            this.attemptColl.fetch(this._attemptFetchDefaults());
        },

        _initializeDatePicker: function() {
            // Instantiate datepicker using custom date ranges
            $("#summaryrange", this.$el).daterangepicker(
                window.ExS.datePickerOptions(this.syncOn.get("minDate"),
                    this.syncOn.get("maxDate")));

            //Set the initial state of the picker label
            $("#summaryrange input", this.$el).val(
                this.syncOn.get("currentStartDate").format(
                    this.displayDateFormat) + " - " +
                 this.syncOn.get("currentEndDate").format(
                    this.displayDateFormat));
        },

        // Update datepicker settings
        // Run only after datepicker element has been created
        updateDatePickerRanges: function(minDate, maxDate) {
            this.afterRender.done(function() {
                this._initializeDatePicker();
            });
        },

        _percentageValue: function(value, total) {
            return (value * 100 / total).toFixed(2);
        },

        _convertAttemptsForDisplay: function(models) {
            return _.map(models, _.bind(function(row) {
                var total = row.total_attempts;
                row.originalName = row.exercise;
                row.exercise = window.ExS.normalizeName(row.exercise);
                row.correct_attempts = this._percentageValue(
                    row.correct_attempts, total);
                row.wrong_attempts = this._percentageValue(
                    row.wrong_attempts, total);
                row.time_taken = (row.time_taken / total).toFixed(0);
                return row;
            }, this));
        },

        _convertProficiencyForDisplay: function(models) {
            return _.map(models, _.bind(function(row) {
                row.originalName = row.exercise;
                row.exercise = window.ExS.normalizeName(row.exercise);
                row.earned_proficiency = this._percentageValue(
                    row.earned_proficiency, row.total_users);
                return row;
            }, this));
        },

        // Render elements and attach jquery plugins
        render: function() {
            this._cleanup();

            $(this.template({
                empty: this.attemptColl.length === 0,
                units: ["", "", "%", "%", "s"],
                orderTotal: ["exercise", "total_attempts",
                    "correct_attempts", "wrong_attempts", "time_taken"],
                orderProf: ["exercise", "total_users", "earned_proficiency"],
                totalData: this._convertAttemptsForDisplay(
                    _.pluck(this.attemptColl.models, "attributes")),
                profData: this._convertProficiencyForDisplay(
                    _.pluck(this.proficiencyColl.models, "attributes"))
            })).appendTo(this.$el);

            $("#summ-table", this.$el).dataTable(_.extend(
                _.clone(this.dataTableConfig),
                {
                    "aoColumns": [
                        null,
                        null,
                        { "sType": "percent" },
                        { "sType": "percent" },
                        { "sType": "sec" }
                    ]
                }
            ));

            $("#prof-table", this.$el).dataTable(_.extend(
                _.clone(this.dataTableConfig),
                {
                    "aoColumns": [
                        null,
                        null,
                        { "sType": "percent" }
                    ]
                }
            ));

            // initialize tabs
            $('#total-tabs a', this.$el).click(function (e) {
                e.preventDefault();
                $(this).tab('show');
            })

            this._initializeDatePicker();

            this.afterRender.resolveWith(this);
            return this;
        }
    });

    _.extend(window.ExS, {
        SummaryTableView: SummaryTableView
    });

})();
