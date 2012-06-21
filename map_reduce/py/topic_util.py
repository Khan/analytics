"""Utility functions for processing a topic tree object."""


def find_exercise_topics(tree):
    """Given a tree object, find all topic nodes with at least one exercise.

    Returns a dictionary of the form { topic_name => topic_object }
    """
    result = {}
    if tree['kind'] == 'Topic':
        is_exercise_topic = False
        for child in tree['children']:
            if child['kind'] == 'Exercise':
                is_exercise_topic = True
            if child['kind'] == 'Topic':
                result.update(find_exercise_topics(child))
        if is_exercise_topic:
            result.update({tree['id']: tree})
    return result


def exercise_topic(tree):
    """Return a dictionary { exercise_name => topic_name }.

    If an exercise is ever in more than one topic, this will
    not handle that.  It assumes ex -> topic is 1-1.
    """
    result = {}

    for topic_name, topic in find_exercise_topics(tree).iteritems():
        for child in topic['children']:
            if child['kind'] == 'Exercise':
                result[child['name']] = topic_name

    return result


def topic_exercise(tree):
    """Return a dictionary of the form {topic_name => list of ex_names}."""
    result = {}
    for topic_name, topic in find_exercise_topics(tree).iteritems():
        result[topic_name] = []
        for child in topic['children']:
            if child['kind'] == 'Exercise':
                result[topic_name].append(child['name'])
    return result
