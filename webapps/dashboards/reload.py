#!/usr/bin/env python
from livereload.task import Task

Task.add('static/css/*.css')
Task.add('*.py')
Task.add('static/css/third_party/*.css')
Task.add('static/js/third_party/*.js')
Task.add('static/js/*.js')
Task.add('templates/*.html')
Task.add('templates/gae-stats/*.html')

