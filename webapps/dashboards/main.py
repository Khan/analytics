#!/usr/bin/env python

"""A dashboard webapp.

This hosts the following dashboards:
- daily exercise statistics over time
- a daily report of videos watched
- learning efficiency and retention from exercises

It will house more dashboards for fundamental metrics we want to track.
"""

import optparse

import flask

app = flask.Flask(__name__)


@app.route('/')
def list_dashboards():
    return flask.render_template('index.html')


@app.route('/videos')
def videos_dashboard():
    return flask.render_template('daily-video-stats.html')


@app.route('/exercises')
def exercises_dashboard():
    return flask.render_template('daily-ex-stats.html')


@app.route('/learning')
def learning_dashboard():
    return flask.render_template('learning-stats.html')


def main():
    parser = optparse.OptionParser()
    parser.add_option("-d", "--debug", action="store_true", dest="debug",
                      help="Whether to run in debug mode "
                           "(only accessible by localhost and autoreloads)")
    parser.add_option("-p", "--port", type="int", default=-1,
                      help="The port to run on (defaults to 5000 for debug, "
                           "else defaults to 80)")
    options, _ = parser.parse_args()

    app.debug = options.debug
    port = options.port
    if options.debug:
        if port == -1:
            port = 5000
        app.run(port=port)
    else:
        if port == -1:
            port = 80
        app.run(host='0.0.0.0', port=port)

if __name__ == '__main__':
    main()
