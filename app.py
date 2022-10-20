import argparse
import csv
import io

import apache_beam as beam

from apache_beam.io import WriteToText
from apache_beam.io.fileio import MatchFiles, ReadMatches
from apache_beam.options.pipeline_options import PipelineOptions
from transforms import CompositeFilterTransactions


def run(argv=None):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv',
        help='Input csv file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        default='output/results.jsonl.gz',
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    def get_csv_reader(readable_file):
        return csv.DictReader(io.TextIOWrapper(readable_file.open()))

    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        # Read CSV into pcollection
        read_records = (p
                        | MatchFiles(known_args.input)
                        | ReadMatches()
                        | beam.FlatMap(get_csv_reader))

        perform_transforms = read_records | CompositeFilterTransactions()

        # shard name is set to '' to create the output in desired format
        export = perform_transforms | WriteToText(known_args.output,
                                                  num_shards=1,
                                                  shard_name_template='',
                                                  compression_type='gzip')


if __name__ == '__main__':
    run()
