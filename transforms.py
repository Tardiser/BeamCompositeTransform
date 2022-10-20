import apache_beam as beam

from datetime import datetime


class CompositeFilterTransactions(beam.PTransform):
    """ Transform timestamp field to datetime and return record """
    def timestamp_to_datetime(self, record):
        record['timestamp'] = datetime.strptime(record.get('timestamp'), '%Y-%m-%d %H:%M:%S %Z')
        return record

    """ Create a new dict with only wanted columns and format """
    def prepare_for_groupby(self, record):
        record = {
            'date': record.get('timestamp').strftime('%Y-%m-%d'),
            'transaction_amount': float(record['transaction_amount'])
        }
        return record

    """ Return pcollection from composite transform """
    def expand(self, pcol):
        return (pcol
                | "timestamp to datetime" >> beam.Map(self.timestamp_to_datetime)
                | "transaction > 20" >> beam.Filter(
                    lambda record: float(record.get('transaction_amount')) > 20)
                | "year >= 2010" >> beam.Filter(lambda record: record.get('timestamp').year >= 2010)
                | "prepare group by" >> beam.Map(self.prepare_for_groupby)
                | "group by" >> beam.GroupBy(lambda s: s['date']).aggregate_field(
                    lambda record: record['transaction_amount'], sum, 'total_amount')
                | "tuple to dictionary" >> beam.Map(
                    lambda record: {'date': record.key, 'total_amount': round(record.total_amount, 2)})
                )
