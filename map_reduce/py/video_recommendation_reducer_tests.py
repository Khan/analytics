import StringIO
import unittest

import video_recommendation_reducer

INPUT = []
OUTPUT = None
OLD_IN = None
OLD_OUT = None


class Utils(unittest.TestCase):

    def setUp(self):
        """Set up the necessary variables for the tests."""
        global INPUT, OUTPUT, OLD_IN, OLD_OUT
        OLD_IN = video_recommendation_reducer._in
        OLD_OUT = video_recommendation_reducer._out

        video_recommendation_reducer._in = INPUT
        video_recommendation_reducer._out = OUTPUT = StringIO.StringIO()

    def tearDown(self):
        """Clean up stuff left after the test"""
        global INPUT
        video_recommendation_reducer._out.close()

        video_recommendation_reducer._in = OLD_IN
        video_recommendation_reducer._out = OLD_OUT

        # Clean this up; we don't need it anymore
        INPUT = []

    def run_reducer(self):
        global OUTPUT
        """Make reducer use our input, output files, run it, close them."""

        video_recommendation_reducer.main()

        OUTPUT = OUTPUT.getvalue().split("\n")
        if OUTPUT[-1] == "":
            OUTPUT = OUTPUT[:-1]  # Get rid of empty line

    def write_input(self, input_to_write):
        """Put input in a list for reducer to use as input, in proper format.
        (We use a list instead of a file so we don't need file I/O)

        input_to_write -- list of tuples of (user, video, timestamp)

        """
        for line in input_to_write:
            INPUT.append("%s\t%s\t%s\n" % line)

    def read_output(self):
        """Read the file output by the reducer.

        Returns a list of tuples of (videoi, videoj, indicatori, indicatorj)

        """
        to_return = []
        for line in OUTPUT:
            split_line = line.rstrip().split("\t")
            self.assertEqual(4, len(split_line))  # Ensure output is valid
            to_return.append(tuple(split_line))

        return to_return

    def generic_test(self, to_write, expected):
        global INPUT

        """Test reducer with a specific input (same format as write_input_file)
        and expected output (same format as read_output_file, any order)

        """
        self.write_input(to_write)

        self.run_reducer()

        result = self.read_output()

        self.assertEqual(set(result), set(expected))

    def duplicate(self, videos):
        """Given a list of format similar to read_output_file's but with only
        videoi, videoj, indicator_ij, indicator_ji, output the same list but
        also with videoj, videoi, indicator_ji, indicator_ij
        """
        videos_copy = list(videos)  # Copy videos so we can modify it in loop
        for video in videos:
            (video_i, video_j, i, j) = video
            videos_copy.append((video_j, video_i, j, i))
        return videos_copy


class VideoRecommenderTest(Utils):

    def test_one_user_videos(self):
        """Test the reducer if we have only one user"""
        to_write = [("user1", "video1", "1339627945.362114"),
                    ("user1", "video2", "1339123414.241234"),
                    ("user1", "video3", "1339511343.342424")]

        expected = self.duplicate(
                    [("video1", "video2", "0", "1"),
                     ("video1", "video3", "0", "1"),
                     ("video2", "video3", "1", "0")])
        self.generic_test(to_write, expected)

    def test_last_user_has_one(self):
        """Test the case where the last user in the list has only 1 watch."""
        to_write = [("user1", "video1", "2232134.1245"),
                    ("user1", "video3", "2341234.2344"),
                    ("user1", "video4", "1234123.2344"),
                    ("user2", "video2", "5324352.1345"),
                    ("user2", "video4", "4513431.1234"),
                    ("user2", "video5", "1241234.4213"),
                    ("user3", "video1", "1541451.1324")]

        expected = self.duplicate(
                   [("video1", "video3", "1", "0"),
                    ("video1", "video4", "0", "1"),
                    ("video3", "video4", "0", "1"),
                    ("video2", "video4", "0", "1"),
                    ("video2", "video5", "0", "1"),
                    ("video4", "video5", "0", "1")])

        self.generic_test(to_write, expected)

    def test_all_users_have_one(self):
        """Test the case where all users have only 1 video watched."""
        to_write = [("user1", "video1", "2232134.1245"),
                    ("user2", "video2", "1243123.1234"),
                    ("user3", "video3", "1341234.1234")]
        expected = []

        self.generic_test(to_write, expected)

    def test_all_users_have_multiple(self):
        """Test the case where all users have multiple videos watched."""

        to_write = [("user1", "video1", "2232134.1245"),
                    ("user1", "video3", "2341234.2344"),
                    ("user1", "video4", "1234123.2344"),
                    ("user2", "video2", "5324352.1345"),
                    ("user2", "video4", "4513431.1234"),
                    ("user2", "video5", "1241234.4213"),
                    ("user3", "video1", "1541451.1324"),
                    ("user3", "video2", "1234432.1234")]

        expected = self.duplicate(
                   [("video1", "video3", "1", "0"),
                    ("video1", "video4", "0", "1"),
                    ("video3", "video4", "0", "1"),
                    ("video2", "video4", "0", "1"),
                    ("video2", "video5", "0", "1"),
                    ("video4", "video5", "0", "1"),
                    ("video1", "video2", "0", "1")])

        self.generic_test(to_write, expected)

if __name__ == "__main__":
    unittest.main()
