(function() {

    parseGenerator = function(property, fn) {
        return function(response, options) {
            return _.map(response[property], function(data) {
                return new fn(data);
            });
        };
    };

    /**
     * Backbone model for date ranges
     * Every date is a Moment.js object
     */
    var DateRangeModel = Backbone.Model.extend({
        url: "/db/exercise-summary/date-ranges",

        defaults: {
                dates: []
        },

        // Convert dates to moment objects
        parse: function(response, options) {
            return {
                dates: _.map(response.dates, function(date) {
                    return moment(date);
                })
            };
        },

        minDate: function() {
            return _.min(this.get("dates"));
        },

        maxDate: function() {
            return _.max(this.get("dates"));
        },

    });

    /**
     * Backbone model for exercise summary.
     */
    var SummaryModel = Backbone.Model.extend({
        url: "",
        defaults: {
            time_taken: 0,
            correct_attempts: 0,
            wrong_attempts: 0,
            total_attempts: 0,
            exercise: "",
            isPerseus: false
        },

        initialize: function(attr, options) {
            Backbone.Model.prototype.initialize.call(this, attr, options);
            this.set({
                total_attempts: this.get("correct_attempts") +
                    this.get("wrong_attempts"),
            });
        },

    });

    /**
     * Backbone model for problem type as returned from the server.
     * Left as a separate model to allow for further extensibility
     *  i.e. integrating perseus exercises which do not declare problem type
     */
    var SubExerciseClassificationModel = SummaryModel.extend({
        defaults: {
            time_taken: 0,
            correct_attempts: 0,
            wrong_attempts: 0,
            total_attempts: 0,
            sub_exercise_type: "",
            exercise: "",
            isPerseus: false
        },

        initialize: function(attr, options) {
            SummaryModel.prototype.initialize.call(this, attr, options);
            // TODO(robert): Remove once the hive query populates this value
            this.set({
                isPerseus: this.get("sub_exercise_type")
                    .search(/^x[0-9a-f]{8}$/) !== -1
            });
            // Some static exercises might have null seed. Maybe old data?
            // TODO(robert): investigate null seed value for static exercises
            if(!this.get("sub_exercise_type")) {
                this.set({
                    sub_exercise_type: "0"
                });
            }
        }
    });

    /**
     * Models entry in exercise_proficiency_summary
     * TODO(robert): how will it look with
     *  new recommendations (Practice/Mastery)?
     */
    var ProficiencyModel = SummaryModel.extend({
        defaults: {
            total_users: 0,
            earned_proficiency: 0,
            exercise: ""
        },

        /**
         * It can either be created directly (first case) or
         *  from collection parse (second case)
         * Behaviour of this function is due to the fact that
         *  that we are not using standard backbone way of dealing
         *  with models. (They are not auto synced to server, since
         *  it's a readonly application)
         */
        parse: function(response, options) {
            return response.proficiency_data || response;
        },

        url: function() {
            return "data/exercise-proficiency-summary/" +
                this.get("exercise");
        }
    });

    /**
     * Collection holding SubExerciseClassificationModel models
     * Represents Exercise summary as returned by the server
     * @param {string} exercise name of the exercise for which the data
     *     is to be fetched. Passed in options object.
     */
    var SubExerciseCollection = Backbone.Collection.extend({
        model: SubExerciseClassificationModel,

        initialize: function(models, options) {
            Backbone.Collection.prototype
                .initialize.call(this, models, options);
            this.exercise = options.exercise;
        },

        /**
         * In order to avoid sync and fetch events from the model
         * we use reset on the collection and manually instantiate models
         * Bind to "reset" event to know when data is ready
         */
        parse: parseGenerator("exercise_data", SubExerciseClassificationModel),

        url: function() {
            return "/data/exercise-summary/" + this.exercise;
        },

        /**
         * Reduce data to produce exercise level summary
         * Done client side because data per problem type
         *  might be needed as well
         * It's not possible for this collection to be empty
         *  as long as only valid requests are executed
         */
         prepareSummary: function() {
            var summData = this.reduce(function(acc, subExerciseGroup) {
                acc.time_taken += subExerciseGroup.get("time_taken");
                acc.correct_attempts +=
                    subExerciseGroup.get("correct_attempts");
                acc.wrong_attempts += subExerciseGroup.get("wrong_attempts");
                acc.total_attempts += subExerciseGroup.get("total_attempts");
                return acc;
            }, {
                time_taken: 0,
                correct_attempts: 0,
                wrong_attempts: 0,
                total_attempts: 0
            });
            summData.exercise = this.at(0).get("exercise");
            summData.isPerseus = this.at(0).get("isPerseus");
            return new SummaryModel(summData);
        },

        // Function to map models to format that fits graphing functions
        _graphDataMap: function(subExerciseGroup) {
            var dataSeries = [{
                name: "correct",
                attempts: subExerciseGroup.get("correct_attempts")
            },
            {
                name: "wrong",
                attempts: subExerciseGroup.get("wrong_attempts")
            }];
            var dataObject = {
                isPerseus: subExerciseGroup.get("isPerseus"),
                total: subExerciseGroup.get("total_attempts"),
                timeTaken: subExerciseGroup.get("time_taken"),
                series: dataSeries,
                exercise: subExerciseGroup.get("exercise"),
                subExerciseGroup: subExerciseGroup.get("sub_exercise_type")
            };
            return dataObject;
        },

        // Return part of the summary data that is needed for graph plotting
        prepareSummaryGraphData: function() {
            if(this.length) {
                return this._graphDataMap(this.prepareSummary());
            } else {
                return null;
            }
        },

        // Extracts data for plotting for each problem type
        // All problem types that contribute < 1% of total attempts
        //  are excluded
        // Results are sorted by total attempts number
        // Returns list of objects as returned by _graphDataMap
        prepareGraphData: function() {
            var total = 0;
            return this.chain().map(this._graphDataMap)
            .tap(function(data) {
                total = _.reduce(data, function(acc, elem) {
                    return acc += elem.total;
                }, 0);
            })
            .filter(function(elem) {
                return elem.total > (total / 100);
            })
            .sortBy(function(elem) {
                return -elem.total;
            }).value();
        }

    });

    // Holds whole exercise collection
    var Exercises = Backbone.Collection.extend({
        model: SummaryModel,
        url: "/data/exercise-summary/all",

        parse: parseGenerator("exercise_data", SummaryModel),

    });

    // Holds whole proficiencies collection
    var Proficiencies = Backbone.Collection.extend({
        model: ProficiencyModel,
        url: "/data/exercise-proficiency-summary/all",

        parse: parseGenerator("proficiency_data", ProficiencyModel),

    });

    // Model representing all stateful information in the application.
    // Simplifies synchronisation and allows using generic backbone events.
    // Makes underlying possible date ranges transparent to users.
    var CurrentParametersModel = Backbone.Model.extend({
        url: "",
        defaults: {
            exercise         : "",
            currentStartDate : moment("01/01/2010"),
            currentEndDate   : moment(),
            minDate          : moment("01/01/2010"),
            maxDate          : moment()
        },

        initialize: function(attr, options) {
            this.possibleDates = new window.ExS.DateRangeModel();
            this.listenTo(this.possibleDates, "change:dates", this._onMinMaxChange);
            this.listenTo(this.possibleDates, "error", this._onRangeError);

            this.possibleDates.fetch(window.ExS.fetchDefaults);
        },

        _onMinMaxChange: function(model, dates) {
            this.set({
                minDate: model.minDate(),
                maxDate: model.maxDate()
            });
        },

        _onRangeError: function() {
            this.trigger("error");
        }
    });

    _.extend(window.ExS, {
        SubExerciseCollection          : SubExerciseCollection,
        Exercises                      : Exercises,
        Proficiencies                  : Proficiencies,
        ProficiencyModel               : ProficiencyModel,
        SubExerciseClassificationModel : SubExerciseClassificationModel,
        SummaryModel                   : SummaryModel,
        DateRangeModel                 : DateRangeModel,
        CurrentParametersModel         : CurrentParametersModel
    });

})();
