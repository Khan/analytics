/**
 * Badge Summary dashboard. Shows all information that you might ever want to
 *  know about badge statistics on Khan Academy
 */

!function() {

    // Database URLs
    var DB_URL = "http://107.21.23.204:27080/report/";
    var MONGO_COLLECTION_URL = DB_URL + "topic_old_key_name/";
    var QUERY_URL = "/data/badge-summary/";

    Handlebars.registerPartial("badge-stats",
        Handlebars.compile($("#badge-tpl").html().trim()));

    // Convert numbers to ordinals (for display)
    Handlebars.registerHelper("ordinal", function(str) {
        str = str + "";
        var suffix = "";
        if(str.length === 2 && str[0] === "1") {
            suffix = "th";
        } else {
            switch (str.slice(-1)) {
                case "1":
                    suffix = "st";
                    break;
                case "2":
                    suffix = "nd";
                    break;
                case "3":
                    suffix = "rd";
                    break;
                default:
                    suffix = "th";
                    break;
            }
        }
        return str + suffix;
    });

    var percentify = function(number, total) {
        return (number * 100 / total).toFixed(3);
    };

    var TopicOldKeyModel = Backbone.Model.extend({
        defaults: {
            slug: "",
            title: "",
            old_key_name: "",
            standalone_title: ""
        },

        url: "",

        parse: function(response) {
            return response.result;
        }
    });

    var TopicOldKeyCollection = Backbone.Collection.extend({
        model: TopicOldKeyModel,
        url: MONGO_COLLECTION_URL + "_find?callback=?&batch_size=2000",

        parse: function(response) {
            return _.map(response.results, function(data) {
                return new TopicOldKeyModel(data);
            });
        }
    });

    var BadgeModel = Backbone.Model.extend({
        defaults: {
            total_awarded: 0,
            unique_awarded: 0,
            context_name: "",
            badge_name: "",
            total_points_earned: 0
        },

        url: "",

        parse: function(response) {
            return response.badges;
        }
    });

    var Badges = Backbone.Collection.extend({
        model: BadgeModel,

        url: function() {
            return QUERY_URL + this.badge.badge_name;
        },

        initialize: function(models, options) {
            Backbone.Collection.prototype
                .initialize.call(this, models, options);
            this.badge = options.badge;
        },

        parse: function(response) {
            return _.map(response.badges, function(badge) {
                return new BadgeModel(badge);
            });
        },

        normalizeBadge: function(badge, topics, metadata) {
            var newProperties = {};
            var badgeName = badge.get("badge_name");
            var topicMeta = metadata.topic_exercise_badges;
            // Transform "topic_exercise" badges to have usable names
            var isTopicExercise = badgeName.indexOf("topic_exercise_");
            if(isTopicExercise !== -1) {
                // 15 is length of "topic_exercise_"
                var lookupTopicKey = badgeName.substring(15);
                var relatedTopic = topics.find(function(topic) {
                    return topic.get("old_key_name") === lookupTopicKey;
                });

                // Any badge that fails this check has no matching topic
                if(relatedTopic) {

                    var iconName = "default";
                    if(_.contains(topicMeta.custom_icons,
                        relatedTopic.get("slug"))) {
                        iconName = relatedTopic.get("slug");
                    }
                    iconPrefix = topicMeta.icon_src + iconName;

                    newProperties = {
                        badge_display_name: "Topic: " +
                            relatedTopic.get("title"),
                        description: topicMeta.description +
                            relatedTopic.get("standalone_title"),
                        context: metadata.contexts["0"],
                        category: _.extend(_.clone(
                            metadata.categories["5"]), {
                                large_icon_src: iconPrefix +
                                    topicMeta.icon_suffixes.large_icon,
                                icon_src: iconPrefix +
                                    topicMeta.icon_suffixes.icon,
                                compact_icon_src: iconPrefix +
                                    topicMeta.icon_suffixes.compact_icon,
                                medium_icon_src: iconPrefix +
                                    topicMeta.icon_suffixes.compact_icon,
                        }),
                        points: 0,
                        triggers: []
                    };
                }
            } else {
                var badgeData = metadata.badges[badgeName];
                // Anything that fails this check is a custom badge
                if(badgeData) {
                    newProperties = {
                        badge_display_name: badgeData.description,
                        category: metadata.categories[
                            badgeData.category + ""],
                        points: badgeData.points,
                        description: badgeData.extended_descritpion,
                        context: metadata.contexts[
                            badgeData.context_type + ""],
                        triggers: badgeData.triggers
                    };
                }
            }
            return newProperties;
        },

        // Normalize all badge models with topic information and badge metadata
        mergeMetadata: function(topics, metadata) {
            var filteredBadges = this.map(_.bind(function(badge) {
                var newProperties = this.normalizeBadge(badge, topics,
                    metadata);

                // Trust me, there's no other way
                if(!_.isEmpty(newProperties)) {
                    badge.set(newProperties);
                    return badge;
                }
            }, this)).filter(function(badge) {
                return badge;
            });

            this.reset(filteredBadges);
        },

        // Finds ordering for a badge within its category and context
        //  and calculate subtotals for each category and context
        createRollupsForCategories: function() {
            var modelAttributes = _.pluck(this.models, "attributes");
            var categories = _.uniq(_.chain(modelAttributes)
                .pluck("category").pluck("category").value());
            var contexts = _.uniq(_.chain(modelAttributes)
                .pluck("context").pluck("context").value());
            _.each({
                category: categories,
                context: contexts
            }, _.bind(function(group, name) {
                _.each(group, _.bind(function(value) {
                    var amount = 0;
                    var filtered = _.chain(this.models).filter(
                        function(model) {
                            return model.get(name)[name] === value;
                    }).tap(function(models) {
                        amount = models.length;
                    });

                    _.each(["total_awarded", "unique_awarded"],
                        function(metric) {
                            var count = filtered.reduce(function(acc, badge) {
                                acc += badge.get(metric);
                                return acc;
                        }, 0).value();

                        var label = name + "_" + metric + "_order";
                        var totalLabel = name + "_" + metric + "_total";
                        filtered.sortBy(function(model) {
                            return -model.get(metric);
                        }).zip(_.range(1, amount + 1))
                        .each(function(badge) {
                            badge[0].set(label, badge[1]);
                            badge[0].set(totalLabel, count);
                        });
                    });
                }, this))
            }, this));
        },

        // Attempt at making display names of missing data more understandable
        _convertContextName: function(name) {
            var newName = name;
            if(name === "\\N") {
                newName = "None";
            } else if(!name) {
                newName = "None";
            }
            return newName;
        },

        // Calculate percentages for different contexts of same badge
        createRollupsPerBadgeContext: function() {
            var totalUnique = 0;
            var totalAwarded = 0;
            return _.chain(this.models).pluck("attributes")
                .tap(function(contexts) {
                    var totals = _.reduce(contexts, function(acc, elem) {
                        acc[0] += elem.total_awarded;
                        acc[1] += elem.unique_awarded;
                        return acc;
                    }, [0, 0]);
                    totalAwarded = totals[0];
                    totalUnique = totals[1];
                }).map(_.bind(function(context) {
                    context.total_percentage = percentify(
                        context.total_awarded, totalAwarded);
                    context.unique_percentage = percentify(
                        context.unique_awarded, totalUnique);
                    context.context_name = this._convertContextName(
                        context.context_name);
                    if(!this.badge.points) {
                        delete context.total_points_earned;
                    }
                    return context;
            }, this)).value();
        }
    });

    var BadgeSummaryView = Backbone.View.extend({
        tagName: "div",
        id: "badges",
        className: "badges container",

        template: Handlebars.compile($("#badge-container").html().trim()),
        errorTemplate: Handlebars.compile($("#error-tpl").html().trim()),
        controlsTpl: Handlebars.compile($("#badge-controls").html().trim()),
        modalTpl: Handlebars.compile($("#badge-modal-tpl").html().trim()),
        popupTpl: Handlebars.compile($("#badge-popup-tpl").html().trim()),
        // Cache loader as jquery object since it's the only way it is used.
        $loader: $(Handlebars.compile(
            $("#loading-tpl").html().trim())().trim()),
        dataTableConfig: {
            "sDom": ["<'row'<'span6'l><'span6'f>r>t",
            "<'row'<'span6'i><'span6'p>>"].join(""),
            "sPaginationType": "bootstrap",
            "oLanguage": {
                "sLengthMenu": "_MENU_ records per page"
            },
            "aaSorting": [[1,'desc']]
        },
        dbDateFormat: "YYYY-MM-DD",
        fetchDefaults: {
            reset: true,
            localCache: true,
            cacheTTL: 12
        },

        events: {
            "click .tile": "_onBadgeClick",
            "keyup #filter-query": "_onKeywordSearch",
            "click #filter-query ~ .add-on": "_clearSearch"
        },

        // Listen to collection events
        // Fetch topic names
        initialize: function() {
            _.extend(this, Backbone.Events);
            // Get rid of all local binds
            _.bindAll(this);
            this.badges = new Badges([], {badge: {
                badge_name: "all"
            }});
            this.topicKeys = new TopicOldKeyCollection();
            this.badgeContexts = new Badges([], {});
            this.sortBy = "";
            this.pastDays = -1;
            this.filter = [];
            this.userCancelled = false;
            this.badgesFetchXHR = {};
            this.topicsFetch = $.Deferred();
            this.badgesMetaFetch = $.Deferred();

            this.listenTo(this.badges, "reset", this.processDeferred(
                "badgesFetch", "resolve"));

            this.listenTo(this.badges, "error", this.processDeferred(
                "badgesFetch", "reject"));

            this.listenTo(this.topicKeys, "reset", this.processDeferred(
                "topicsFetch", "resolve"));

            this.listenTo(this.topicKeys, "error", this.processDeferred(
                "topicsFetch", "reject"));

            // Listen to load events of badge context view
            this.listenTo(this.badgeContexts, "error", this._error);
            this.listenTo(this.badgeContexts, "reset", this._renderBadgeModal);

            this.topicKeys.fetch(
                _.defaults({cacheTTL: 72}, this.fetchDefaults));

            // Fetch badge metadata information
            //  generated by tools/parse_badges.py
            this.badgesMetaFetch = $.getJSON("/static/badges_meta.json",
                _.defaults({cacheTTL: 72}, this.fetchDefaults));
            this.badgesMetaFetch.done(_.bind(function(data) {
                    this.badgesMeta = data.contents;
            }, this))
        },

        processDeferred: function(deferred, action) {
            return function() {
                this[deferred][action]();
            };
        },

        // Change displayed badges.
        // Since the UI is non blocking we have to keep track of last request
        //  and cancel it if necessary.
        onChangeDisplay: function(pastDays, sortBy, filterBy) {
            if(pastDays !== this.pastDays) {
                // Check if another request is currently running
                if(this.badgesFetchXHR && this.badgesFetchXHR.readyState > 0 &&
                    this.badgesFetchXHR.readyState < 4) {
                        this.userCancelled = true;
                        this.badgesFetchXHR.abort();
                } else {
                    this._toggleLoader();
                }

                this.pastDays = pastDays;

                this.badgesFetch = $.Deferred()
                // Any .abort() call causes fail hence there is need to
                //  distinguish between system and user triggered one.
                $.when(this.badgesFetch, this.topicsFetch,
                    this.badgesMetaFetch).then(_.bind(function() {
                        this.userCancelled = false;
                        this.badges.mergeMetadata(this.topicKeys,
                            this.badgesMeta);
                        this.badges.createRollupsForCategories();
                        this.render();
                }, this), _.bind(function() {
                    if(!this.userCancelled) {
                        this._error();
                    }
                }, this));

                var dateRange = {};
                if(this.pastDays) {
                    dateRange.data = {
                        start_date: moment().subtract("days", this.pastDays)
                            .format(this.dbDateFormat),
                        end_date: moment().format(this.dbDateFormat)
                    }
                }

                this.badgesFetchXHR = this.badges.fetch(_.extend(
                    _.clone(this.fetchDefaults), dateRange));
            }
            this.sortBy = sortBy;
            this.filterBy = filterBy;
            this._sortBadgesBy(this.sortBy);
            this._filterBadgesBy(this.filterBy);
        },

        // Handle call to sorting badges
        _sortBadgesBy: function(sortBy, ascending) {
            var sortAscending = ascending ? ascending : false;
            if(!sortBy || sortBy === "none") {
                $("a[href^=#sort\\/]").removeClass("active");
            } else {
                $("a[href$=\\/" + sortBy + "]").button("toggle");
            }
            $("#badge-tiles", this.$el).isotope({
                sortBy: sortBy,
                sortAscending: sortAscending
            });
        },

        // Show only selected badges
        _filterBadgesBy: function(filterBy) {
            $("a[href^=#filter\\/]").removeClass("active");
            var filter = this._filterToClassSelector(filterBy);
            $("#badge-tiles", this.$el).isotope({
                filter: filter
            });

            if(filter !== "*") {
                _.each(filterBy, function(group) {
                    _.each(group, function(filter) {
                        $("a[href$=\\/" + filter + "]").button("toggle");
                    });
                });
            }
        },

        // Show badge modal
        _onBadgeClick: function(ev) {
            var $target = $(ev.currentTarget);
            // data-badge is an attributes hash of underlying badge model
            this.badgeContexts.badge = this.badges
                .get($target.data("badge")).attributes;

            $("#badge-modal").html(this.$loader);
            $.magnificPopup.open({
                items: {
                    src: $("#badge-modal"),
                    type: "inline"
                },
                callbacks: {
                    // Fix for moving navbar on popup
                    beforeOpen: function() {
                        $(".navbar-inner").toggleClass("block-fixed-fix");
                        $("#scrollUp").toggleClass("non-block-fixed-fix");
                    },
                    afterClose: function() {
                        $(".navbar-inner").toggleClass("block-fixed-fix");
                        $("#scrollUp").toggleClass("non-block-fixed-fix");
                    }
                },
                removalDelay: 300,
                mainClass: "popup-slide-container",
                fixedContentPos: true,
                fixedBgPos: true
            });
            this.badgeContexts.fetch(this.fetchDefaults);
        },

        // Filter tiles by specific phrase
        _onKeywordSearch: _.debounce(function(ev) {
            var query = $("#filter-query").val().toLowerCase();

            if (query.length === 0) {
                // show all
               this._filterBadgesBy(this.filterBy);
            } else {
                // build jquery obj
                var tokens = query.split(" ");
                var itemsToShow = $(".tile:not(.isotope-hidden)");

                _.each(tokens, function(token) {
                    if(token) {
                        itemsToShow = itemsToShow.filter(function(index) {
                            var text = $(".badge-name", $(this))
                                .text().toLowerCase();
                            return $.fuzzyMatch(text, token).score;
                        });
                    }
                });
                $("#badge-tiles", this.$el).isotope({filter: itemsToShow });
            }
        }, 300),

        _clearSearch: function(ev) {
            $("#filter-query").val("");
            this._onKeywordSearch(ev);
        },

        // Put actual context in the modal
        _renderBadgeModal: function(collection, options) {
            var badgeModalData = {
                contexts: collection.createRollupsPerBadgeContext(),
                badge_name: collection.badge.badge_display_name,
                badge_image: collection.badge.category.large_icon_src,
                badge_description: collection.badge.description,
                badge_has_points: collection.badge.points
            }
            var $badge = $("#badge-modal");
            $badge.html(this.modalTpl(badgeModalData).trim());
            $.magnificPopup.instance.updateItemHTML();
            var columnTypes = [
                null,
                { sType: "comma" },
                { sType: "percent" },
                { sType: "comma" },
                { sType: "percent" }
            ]
            if(collection.badge.points) {
                columnTypes.push(null);
            }
            $("#context-table", $badge).dataTable(_.extend(
                _.clone(this.dataTableConfig), {
                    "aoColumns": columnTypes
            }));
        },

        // Gather data for badge popup
        _generateBadgePopup: function(badge) {
            // Fields needed for the popup
            var attr = badge.attributes;
            var popupBadge = _.chain(attr).pick([
                "category_total_awarded_order",
                "category_unique_awarded_order",
                "context_total_awarded_order",
                "context_unique_awarded_order",
            ]).extend({
                category_total_awarded_percentage: percentify(
                    attr.total_awarded, attr.category_total_awarded_total),
                category_unique_awarded_percentage: percentify(
                    attr.unique_awarded, attr.category_unique_awarded_total),
                context_total_awarded_percentage: percentify(
                    attr.total_awarded, attr.context_total_awarded_total),
                context_unique_awarded_percentage: percentify(
                    attr.unique_awarded, attr.context_unique_awarded_total),
                category_name: attr.category.label.toLowerCase(),
                context_name: attr.context.name.toLowerCase()
            }).value();

            return this.popupTpl(popupBadge).trim();
        },

        // Extract data needed for display
        _convertForDisplay: function(badges, topicMap, metadata) {
            var displayBadges = badges.map(_.bind(function(badge) {
                displayBadge = _.clone(badge.attributes);
                displayBadge.badge_image = badge.get("category").large_icon_src;
                displayBadge.popup_content = this._generateBadgePopup(badge);
                displayBadge.badge = badge.cid;
                displayBadge.filter_classes = badge.get("category").label
                    .toLowerCase().replace(/\s+/g, "_") + " " +
                    badge.get("context").name.toLowerCase()
                        .replace(/\s+/g, "_");
                return displayBadge;
            }, this));
            return displayBadges;
        },

        // Css selectors are... difficult
        // Since we want to have for a filter [a, b]
        //  forall (x,y) in a x b. (x1,y1) V (x2,y2) V...
        //  most of this function is concerned in making cross product
        //  of a and b.
        // It's due to the fact that all elements in a are mutually exclusive
        //  same for b
        _filterToClassSelector: function(filter) {
            var selector = "";
            if(filter[0][0] === "*") {
                selector = "*";
            } else {
                selector = _.chain(filter).reduce(function(acc, group) {
                    var tmpList = [];
                    _.each(acc, function(acc_list) {
                        _.each(group, function(elem) {
                            tmpList.push(acc_list.concat(elem));
                        });
                    })
                    return tmpList.length === 0 ? acc : tmpList;
                }, [[]]).map(function(filter) {
                    return _.map(filter, function(selector) {
                        return "." + selector;
                    }).join(" ");
                }).value().join(", ");
            }
            return selector;
        },

        // Needs fixing, since not always will render in appropriate place
        _error: function() {
            if($.magnificPopup.instance) {
                if(!$.magnificPopup.instance.isOpen) {
                    this._toggleLoader();
                }
                $.magnificPopup.close();
            } else {
                this._toggleLoader();
            }
            $(".tiles", this.$el).parent().parent().remove();
            $("#badge-modal", this.$el).remove();
            $(this.errorTemplate().trim()).appendTo(this.$el);
            $(".alert", this.$el).alert();
        },

        // Turn the loading animation for main view
        _toggleLoader: function() {
            if(this.$loader.is(":visible")) {
                this.$loader.remove();
            } else {
                $(".alert", this.$el).remove();
                $("#badge-tiles", this.$el).parent().parent().remove();
                $("#badge-modal", this.$el).remove();
                this.$loader.appendTo(this.$el);
            }
        },

        // Draw only once to avoid weird behaviour of buttons
        _drawDialogs: _.once(function() {
            $(this.controlsTpl({
                categories: _.chain(this.badgesMeta.categories).values().map(
                    function(category) {
                        displayCategory = {
                            icon: category.icon_src,
                            name: category.label.toLowerCase(),
                            filter: category.label.toLowerCase()
                                .replace(/\s+/g, "_")
                        };
                        return displayCategory;
                }).value(),
                contexts: _.chain(this.badgesMeta.contexts).values().map(
                    function(context) {
                        displayContext = {
                            name: context.name.toLowerCase(),
                            filter: context.name.toLowerCase()
                                .replace(/\s+/g, "_")
                        };
                        return displayContext;
                }).value()
            }).trim()).appendTo(this.$el);

            $.scrollUp({
                animation: "slide"
            });
        }),

        // Render badge tiles
        _drawTiles: function() {
            $(this.template({data: this._convertForDisplay(
                 this.badges, this.topicKeys, this.badgesMeta)}).trim())
            .appendTo(this.$el);

            $("a[href$=\\/" + this.pastDays + "]").button("toggle");

            $("#badge-tiles", this.$el).isotope({
                itemSelector: ".tile",
                layoutMode: "masonry",
                // We should aim to use css transitions but without hardware
                //  acceleration they're painfully slow.
                // With any modern pc (not mine (robert)) this should not be
                //  necessary and can lead to worse performance.
                // animationEngine: "jquery",
                getSortData: {
                    name: function($elem) {
                        return $(".badge-name", $elem).text();
                    },
                    popularity: function($elem) {
                        return parseInt($(".divided-element p", $elem)
                            .first().text());
                    },
                    unique: function($elem) {
                        return parseInt($(".divided-element p", $elem)
                            .last().text());
                    }
                },

                sortBy: this.sortBy,
                filter: this._filterToClassSelector(this.filterBy),
                sortAscending : false
            });

            $(".tile").popover({
                html: true,
                trigger: "hover",
                delay: {
                    show: 300,
                    hide: 200
                },
                container: "body"
            });

            this._filterBadgesBy(this.filterBy);
            this._sortBadgesBy(this.sortBy);
        },

        render: function() {
            this._toggleLoader();
            this._drawDialogs();
            this._drawTiles();

            return this;
        }
    });

    // Provides simple routing which makes it possible
    //  to bookmark specific result views
    var BadgeRouter = Backbone.Router.extend({
        routes: {
            "": "index",
            "days/:days": "limitDate",
            "sort/:sort": "sortBadgesBy",
            "filter/:type/:filter": "filterBadges",
            "days/:days/filter/:filter/sort/:sort": "limitDateSort"
        },
        sortBy: "none",
        pastDays: 0,
        filterBy: [["*"], []],

        index: function() {
            this.sortBy = "none";
            this.pastDays = 0;
            this.filterBy = [["*"], []];
            this.limitDateSort(this.pastDays, this._filterToString(),
                this.sortBy);
        },

        _filterToString: function() {
            return this.filterBy[0].join("|") + ":" +
                this.filterBy[1].join("|");
        },

        _filterFromString: function(filter) {
            var tmp = filter.split(":");
            return [tmp[0] ? tmp[0].split("|") : [],
                tmp[1] ? tmp[1].split("|") : []];
        },

        // Simple wrappers to avoid having to check state when creating urls
        limitDate: function(pastDays) {
            this.limitDateSort(pastDays, this._filterToString(), this.sortBy);
        },

        sortBadgesBy: function(sortBy) {
            this.limitDateSort(this.pastDays, this._filterToString(), sortBy);
        },

        filterBadges: function(type, filterBy) {
            if(_.contains(this.filterBy[type], filterBy)) {
                this.filterBy[type] = _.reject(this.filterBy[type],
                    function(item) {
                        return item === filterBy;
                });
                if(_.every(this.filterBy, _.isEmpty)) {
                    this.filterBy[0].push("*");
                }
            } else {
                this.filterBy[type].push(filterBy);
                this.filterBy[0] = _.reject(this.filterBy[0], function(item) {
                    return item === "*";
                });
            }
            this.limitDateSort(this.pastDays, this._filterToString(),
                this.sortBy);
        },

        limitDateSort: function(pastDays, filterBy, sortBy) {
            // Users can also come directly to this route
            this.filterBy = this._filterFromString(filterBy);
            this.pastDays = parseInt(pastDays, 10);
            this.sortBy = sortBy;
            this.navigate("days/" + this.pastDays + "/filter/" +
                this._filterToString() + "/sort/" + this.sortBy, {
                    replace: true
            });
            this.trigger("refresh", this.pastDays, this.sortBy, this.filterBy);
        }
    });

    $(function() {
        var badgeApp = new BadgeSummaryView();
        badgeApp.$el.appendTo("body");
        var badgeRouter = new BadgeRouter();
        badgeApp.listenTo(badgeRouter, "refresh", badgeApp.onChangeDisplay);
        Backbone.history.start();
    })

}()