"""A dashboard webapp.

This hosts two simple dashboards: daily exercise statistics over time
and a daily report of videos watched. It will house more dashboards for
fundamental metrics we want to track.
"""

import flask

app = flask.Flask(__name__)
app.debug = True  # Show stacktraces and auto-refresh.


@app.route('/')
def list_dashboards():
    # TODO(benkomalo): replace this with a real list!
    return 'Welcome to the dashboards page!'


@app.route('/videos')
def videos_dashboard():
    return flask.render_template('daily-video-stats.html')


@app.route('/exercises')
def exercises_dashboard():
    return flask.render_template('daily-ex-stats.html')


if __name__ == '__main__':
    app.run()
