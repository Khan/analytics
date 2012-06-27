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
    
    def scored_parts(self, parts):
        """Given an input line in an exploded list, generate the expected
        output in an exploded list."""
        score = video_recommendation_pruner.compute_score(
            parts[2], parts[3], parts[4], parts[5])
        return [parts[0], parts[1], str(score)]
        
    def test_single_pair(self):
        line = ["vid1", "vid2", "0", "1", "1", "1"]
        self.fake_input([line])

        self.run_reducer()
        self.assertOutput([["vid1", "vid2", "1.0"]])

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
                "100", "1"
            ]
            input.append(video_i)

            # We expect the last NUM_BEST to win
            if i + video_recommendation_pruner.NUM_BEST > 100:
                expected.append([
                    "vid1",
                    "othervid%s" % i,
                    str(float(i)),
                ])
                
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
        
        expected = [["vid1", "othervid3", "1.0"],
                    ["vid1", "othervid4", "1.0"],
                    ["vid2", "othervid3", "1.0"],
                    ["vid2", "othervid4", "1.0"],
                    ]
        self.assertOutput(expected)


if __name__ == '__main__':
    unittest.main()
