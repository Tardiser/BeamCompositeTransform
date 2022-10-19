import argparse

import apache_beam as beam

from apache_beam.dataframe.io import read_csv
from apache_beam.dataframe.convert import to_dataframe, to_pcollection
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
        default='output/results.csv',
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        records = (p | read_csv(known_args.input))

        to_p = to_pcollection(records) | CompositeFilterTransactions()

        df = to_dataframe(to_p)
        df.to_csv(known_args.output, index=False)


if __name__ == '__main__':
    run()
