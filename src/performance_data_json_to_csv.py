#!/usr/bin/env python
"""
A quick script to extract temporal performance data from a .json file into .csv
format for exploration in R.

See graph_auto_trained_models.py for running instructions.
"""
import csv
import graph_auto_trained_models
import os


def extract_temporal_performance_data(all_temporal_perf_data, csv_path):

    with open(csv_path, 'w') as csv_file:
        csv_writer = csv.writer(csv_file)

        # Write headers
        csv_writer.writerow([
            'job_id',
            'exercise',
            'which_prediction',
            'num_prev_exes',
            # Performance of ALL predictions
            'total_log_liklihood',
            'total_samples',
            'avg_log_liklihood',
            # Performance predictions on only the first problems
            'fp_total_log_liklihood',
            'fp_total_samples',
            'fp_avg_log_liklihood',
        ])

        for exercise, jobs in all_temporal_perf_data.iteritems():
            for job, all_data in jobs.iteritems():
                for num_prev_exes, data in all_data.iteritems():
                    csv_writer.writerow([
                        job,
                        exercise,
                        'original_prediction',
                        int(num_prev_exes),
                        data['original_prediction']['total_log_liklihood'],
                        data['original_prediction']['total_samples'],
                        (data['original_prediction']['total_log_liklihood']
                            / (data['original_prediction']['total_samples']
                                or 0.00001)),
                        data['original_prediction']['fp_total_log_liklihood'],
                        data['original_prediction']['fp_total_samples'],
                        (data['original_prediction']['fp_total_log_liklihood']
                            / (data['original_prediction']['fp_total_samples']
                                or 0.00001))
                    ])

                    csv_writer.writerow([
                        job,
                        exercise,
                        'prediction',
                        int(num_prev_exes),
                        data['prediction']['total_log_liklihood'],
                        data['prediction']['total_samples'],
                        (data['prediction']['total_log_liklihood']
                            / (data['prediction']['total_samples']
                                or 0.00001)),
                        data['prediction']['fp_total_log_liklihood'],
                        data['prediction']['fp_total_samples'],
                        (data['prediction']['fp_total_log_liklihood']
                            / (data['prediction']['fp_total_samples']
                                or 0.00001))
                    ])


def main():
    options = graph_auto_trained_models.parse_command_line()

    all_perf_data, all_temporal_perf_data = (graph_auto_trained_models
        .read_all_performance_data(options.data))

    output_path = os.path.join(options.data, 'temporal_data.csv')
    extract_temporal_performance_data(all_temporal_perf_data, output_path)


if __name__ == "__main__":
    main()
