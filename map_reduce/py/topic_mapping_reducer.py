#!/usr/bin/env python
"""Reducer script to produce topic to parent topics mapping

Input:
    All the lines from json serialized data from the bulkdownloader

    Example:
    g5zfmtoYW4tYWNhZGVteXJmCxIFVG9waWMiKHo3ek5YNE1qMTlxYXRRREZ6eXdmemF5R09kdGg1V0FiZndLYTI1dGcMCxIFVG9waWMiKHFDc2ZpaW5ncmxvb2NiSnJGTmhuQkJvLXhBN1c4RVk5YldHaW1UYXEM	{"key": "ag5zfmtoYW4tYWNhZGVteXJmCxIFVG9waWMiKHo3ek5YNE1qMTlxYXRRREZ6eXdmemF5R09kdGg1V0FiZndLYTI1dGcMCxIFVG9waWMiKHFDc2ZpaW5ncmxvb2NiSnJGTmhuQkJvLXhBN1c4RVk5YldHaW1UYXEM", "standalone_title": "Vi Hart", "backup_timestamp": 1340906706.0, "hide": false, "description": "Recreational mathematics and inspirational videos by resident mathemusician Vi Hart", "ancestor_keys": ["ag5zfmtoYW4tYWNhZGVteXJmCxIFVG9waWMiKHo3ek5YNE1qMTlxYXRRREZ6eXdmemF5R09kdGg1V0FiZndLYTI1dGcMCxIFVG9waWMiKGUzU1lybU9lSTJrMVZNa0tGT0JERlRfUVlhWkhBY1F6UXdnaERZN3AM", "ag5zfmtoYW4tYWNhZGVteXIzCxIFVG9waWMiKHo3ek5YNE1qMTlxYXRRREZ6eXdmemF5R09kdGg1V0FiZndLYTI1dGcM"], "child_keys": ["ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8Yw6zCgAIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8YgarEgAIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8Yha7DgAIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8YyYvGgAIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8Y7dXFgAIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8Y7v_GgAIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8YvMnEgAIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8YtY3CgAIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8Y2qjHgAIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8Y_KHLgAIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8Y7ojJgAIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8Yp-rL2wIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8YmIrM2wIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8Yrtn6uAIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8Y5KnLgAIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8Ys5-52wIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8Yl7rHgAIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8YqP_JgAIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8Yv_HH2wIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8YzJbKgAIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8Y7tXE2wIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8Yp8jHgAIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8Y-NLIgAIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8YvtjFgAIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8Yis7DgAIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8Y34PY6AIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8Yguzo6AIM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8Y99LikAMM", "ag5zfmtoYW4tYWNhZGVteXIPCxIFVmlkZW8YiuK2ogMM"], "created_on": 1327972194.0, "version": "ag5zfmtoYW4tYWNhZGVteXIWCxIMVG9waWNWZXJzaW9uGJSDuKIDDA", "last_edited_by": null, "updated_on": 1340906706.0, "title": "Vi Hart", "extended_slug": "math/vi-hart", "parent_keys": ["ag5zfmtoYW4tYWNhZGVteXJmCxIFVG9waWMiKHo3ek5YNE1qMTlxYXRRREZ6eXdmemF5R09kdGg1V0FiZndLYTI1dGcMCxIFVG9waWMiKGUzU1lybU9lSTJrMVZNa0tGT0JERlRfUVlhWkhBY1F6UXdnaERZN3AM"], "id": "vi-hart"}       # @Nolint

Output format:
    topic_key\ttopic_title\tancestor_keys\tancestor_titles\n
    The keys and titles are sorted from generic to specific
    Example:
    ag5zfmtoYW4tYWNhZGVteXJmCxIFVG9waWMiKHdqaTVBNmNKQVp4a3RZa3gyRTEyNzhlRHZVU3JXNnNwNENsU2xidGIMCxIFVG9waWMiKHFDc2ZpaW5ncmxvb2NiSnJGTmhuQkJvLXhBN1c4RVk5YldHaW1UYXEM	Vi Hart	["ag5zfmtoYW4tYWNhZGVteXIzCxIFVG9waWMiKHdqaTVBNmNKQVp4a3RZa3gyRTEyNzhlRHZVU3JXNnNwNENsU2xidGIM", "ag5zfmtoYW4tYWNhZGVteXJmCxIFVG9waWMiKHdqaTVBNmNKQVp4a3RZa3gyRTEyNzhlRHZVU3JXNnNwNENsU2xidGIMCxIFVG9waWMiKGUzU1lybU9lSTJrMVZNa0tGT0JERlRfUVlhWkhBY1F6UXdnaERZN3AM"]	["The Root of All Knowledge", "Math"]     # @Nolint

"""


import json
import sys


def main():
    topics_map = {}
    # Load all the topics to the topic_map
    for line in sys.stdin:
        line = line.strip()
        (key, topic_json) = line.split('\t')
        topics_map[key] = json.loads(topic_json)
    # Go through topics_map and output the ones with any ancestor keys
    for topic_key, topic_dict in topics_map.iteritems():
        if 'ancestor_keys' in topic_dict:
            ancestors = topic_dict['ancestor_keys'][::-1]
            topic_title = topic_dict['title']
            ancestor_titles = []
            for ancestor_key in ancestors:
                ancestor_titles.append(topics_map[ancestor_key]['title'])
            ancestor_titles.append(topic_title)
            ancestor_keys_str = json.dumps({'keys': ancestors})
            ancestor_title_str = json.dumps({'titles': ancestor_titles})
            s = u"%s\t%s\t%s\t%s" % (topic_key, topic_title,
                                      ancestor_keys_str, ancestor_title_str)

            # some titles, like L'Hopitals rule use non-ASCII characters.
            # Not UTF-8 encoding can throw "UnicodeEncodeError: 'ascii' codec
            # can't encode character u'\xf4' in position..".
            print s.encode("UTF-8")

if __name__ == '__main__':
    main()
