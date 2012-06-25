import StringIO
import unittest

import video_recommendation_pruner


class PrunerTest(unittest.TestCase):

    def setUp(self):
        self.orig_in = video_recommendation_pruner._IN
        self.orig_out = video_recommendation_pruner._OUT
        
        video_recommendation_pruner._IN = []
        video_recommendation_pruner._OUT = StringIO.StringIO()

    def tearDown(self):
        video_recommendation_pruner._IN = self.orig_in
        video_recommendation_pruner._OUT = self.orig_out

    def run_reducer(self):
        video_recommendation_pruner.main()

    def fake_input(self, lines_in_parts):
        for line in lines_in_parts:
            video_recommendation_pruner._IN.append('\t'.join(line))
            
    def get_output_in_lists(self):
        return video_recommendation_pruner._OUT.getvalue().rstrip().split("\n")

    def assertOutput(self, expected_output_in_parts):
        expected_output = map(lambda t: '\t'.join(t),
                              expected_output_in_parts)
        actual_output = self.get_output_in_lists()
        
        # Order doesn't strictly matter, as long as it clusters by the first
        # video key
        self.assertEquals(set(expected_output),
                          set(actual_output))
        
    def test_single_pair(self):
        self.fake_input([["vid1", "vid2", "0", "1", "1", "1"]])

        self.run_reducer()
        self.assertOutput([["vid1", "vid2", "0", "1", "1", "1"]])

    def test_one_video_paired_with_lots(self):
        input = []
        expected = []
        for i in range(1, 101):
            # Video i is named "othervid{i}" and watched i times.
            # after "vid1"
            video_i = [
                "vid1",
                "othervid%s" % i,
                "0", str(i),
                "1", "100"
            ]
            input.append(video_i)

            # We expect the last MAX_BEST to win
            if i + video_recommendation_pruner.MAX_BEST > 100:
                expected.append(video_i)
                
        self.fake_input(input)
        self.run_reducer()
        self.assertOutput(expected)

    def test_two_videos(self):
        input = [["vid1", "othervid3", "0", "1", "2", "1"],
                 ["vid1", "othervid4", "0", "1", "2", "1"],
                 ["vid2", "othervid3", "0", "1", "2", "1"],
                 ["vid2", "othervid4", "0", "1", "2", "1"],
                ]
        self.fake_input(input)
        self.run_reducer()
        self.assertOutput(input)


if __name__ == '__main__':
    unittest.main()
