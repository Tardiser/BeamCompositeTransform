import apache_beam as beam

from datetime import datetime


class CompositeFilterTransactions(beam.PTransform):
    def expand(self, pcol):
        return (pcol
                | beam.Map(lambda row: beam.Row(date=datetime.strptime(row.timestamp, '%Y-%m-%d %H:%M:%S UTC'),
                                                transaction_amount=float(row.transaction_amount)))
                | "filter transaction > 20" >> beam.Filter(lambda row: float(row.transaction_amount) > 20)
                | "exclude before 2010" >> beam.Filter(lambda row: row.date.year >= 2010)
                | "convert date to string" >> beam.Map(lambda row: beam.Row(date=row.date.strftime('%Y-%m-%d'),
                                                                            transaction_amount=row.transaction_amount))
                | "sum by date" >> beam.GroupBy('date').aggregate_field('transaction_amount', sum, 'total_amount')
                | beam.Map(lambda row: beam.Row(date=row.date, total_amount=row.total_amount))
                )

