# Apache Beam Composite Transform Example

This repo contains python scripts for demonstrating Apache Beam Composite Transform knowledge.

## Pre-requisites

You must have apache_beam and its dependencies installed in your environment with gcp and dataframe extras to read from cloud storage and write to csv.
Python version 3.9 is recommended.

To install the required modules, run the below command on terminal:

```sh
pip install apache_beam[gcp,dataframe]==2.42.0
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

## Running the Unit Tests

This repo also contains unit tests for the composite transform created. Feel free to change the inputs and expected results within the unit_tests script.
To run the unit test, you can use:
```sh
python3 -m unittest unit_tests.py
```