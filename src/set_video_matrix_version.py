#!/usr/bin/env python

"""Sets the version for the video recommendations data on production."""

import json
import sys
import oauth_util.consts
import oauth_util.fetch_url as oauth_fetcher


def main(score_type, version):
    print "About to modify video recommendations data:"
    print "  target: ", oauth_util.consts.SERVER_URL
    print "  score_type: ", score_type
    print "  version: ", version
    confirm = raw_input("Proceed? (y/[n]): ")

    if confirm.lower() in ['y', 'yes']:
        # This will throw on 4xx, 5xx
        resp = oauth_fetcher.fetch_url(
                '/api/internal/dev/videorec_version', {
                    'score_type': score_type,
                    'version': version,
                })
        resp = json.loads(resp)
        print "Success!"
        print "To revert back, run:"
        print "set_video_matrix_version.py '%s' '%s'" % (
                score_type, resp['previous'])


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print >>sys.stderr, (
            "Usage:\n"
            "    set_video_matrix_version.py <score_type> <version>")
        sys.exit(1)

    main(sys.argv[1], sys.argv[2])

