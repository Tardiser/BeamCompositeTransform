import unittest

import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from transforms import CompositeFilterTransactions


class CompositeTransformTest(unittest.TestCase):
    def test_composite_transform(self):
        # Static input data to be used in test.
        inputs = [{"timestamp": "2009-01-09 02:54:25 UTC", "transaction_amount": 9.9},
                  {"timestamp": "2011-01-09 02:54:25 UTC", "transaction_amount": 91.287678587567},
                  {"timestamp": "2009-01-09 02:54:25 UTC", "transaction_amount": 4.7},
                  {"timestamp": "2027-01-09 02:54:25 UTC", "transaction_amount": 3.111},
                  {"timestamp": "2000-01-09 02:54:25 UTC", "transaction_amount": 1.00006},
                  {"timestamp": "2010-01-09 02:54:25 UTC", "transaction_amount": 12},
                  {"timestamp": "2010-01-01 00:00:00 UTC", "transaction_amount": 9}]

        # Expected output data after Composite Transform
        expected = [{"date": "2011-01-09", "total_amount": "91.29"}]

        # Create a test pipeline.
        with TestPipeline() as p:

            # Create an input PCollection from list of dicts.
            input = (p | "CreateInput" >> beam.Create(inputs))

            # Apply composite transform to test input data.
            output = input | CompositeFilterTransactions()

            # Assert on the results.
            assert_that(output, equal_to(expected))

    def test_composite_transform_group_sum(self):
        # Static input data to be used in test.
        inputs = [{"timestamp": "2009-01-09 02:54:25 UTC", "transaction_amount": 9},
                  {"timestamp": "2011-01-09 02:54:25 UTC", "transaction_amount": 91},
                  {"timestamp": "2009-01-09 02:54:25 UTC", "transaction_amount": 432566.090},
                  {"timestamp": "2027-01-09 02:54:25 UTC", "transaction_amount": 3},
                  {"timestamp": "2000-01-09 02:54:25 UTC", "transaction_amount": 1},
                  {"timestamp": "2010-01-10 02:54:25 UTC", "transaction_amount": 22},
                  {"timestamp": "2010-01-10 16:54:25 UTC", "transaction_amount": 25.2567},
                  {"timestamp": "2010-01-01 00:00:00 UTC", "transaction_amount": 9}]

        # Expected output data after Composite Transform
        expected = [{"date": "2011-01-09", "total_amount": "91.00"},
                    {"date": "2010-01-10", "total_amount": "47.26"}]

        # Create a test pipeline.
        with TestPipeline() as p:

            # Create an input PCollection from list of dicts.
            input = (p | "CreateInput" >> beam.Create(inputs))

            # Apply composite transform to test input data.
            output = input | CompositeFilterTransactions()

            # Assert on the results.
            assert_that(output, equal_to(expected))
