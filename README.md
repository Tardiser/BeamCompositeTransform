# Apache Beam Composite Transform Example

This repo contains python scripts for demonstrating Apache Beam Composite Transform knowledge.

## Pre-requisites

You must apache_beam library installed in your environment with the gcp extra. Python version 3.9 is recommended.

To install apache_beam library, run the below command on terminal.

```sh
pip install apache_beam[gcp]
```

## Running the Pipeline

To run the pipeline locally, you can use the command below in the same directory with app.py
```sh
python3 app.py
```

Optionally, you can give the desired input file path and output path as arguments. For example:
```sh
python3 app.py \
--input gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv \
--output output/results.csv
```