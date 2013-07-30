// https://github.com/STAR-ZERO/jquery-ellipsis

(function($) {
    $.fn.ellipsis = function(options) {

        var defaults = {
            "row" : 1,
            "char" : "..."
        };

        options = $.extend(defaults, options);

        this.each(function() {
            var $this = $(this);
            var text = $this.text();
            var origHeight = $this.height();

            $this.text("a");
            var rowHeight = $this.height();
            var targetHeight = rowHeight * options.row;

            if (origHeight <= targetHeight) {
                $this.text(text);
                return;
            }

            var start = 1;
            var end = text.length;

            while (start < end) {
                var length = Math.ceil((start + end) / 2);

                $this.text(text.slice(0, length) + options["char"]);

                if ($this.height() <= targetHeight) {
                    start = length;
                } else {
                    end = length - 1;
                }
            }

            $this.text(text.slice(0, start) + options["char"]);
        });

        return this;
    };
}) (jQuery);
