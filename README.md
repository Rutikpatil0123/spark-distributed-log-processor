# Distributed Log Processing & Analytics Pipeline (Apache Spark)

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.x-orange.svg)
![Pytest](https://img.shields.io/badge/Testing-Pytest-green.svg)
![Storage](https://img.shields.io/badge/Format-Parquet-lightgrey.svg)

An end-to-end data processing pipeline built with **PySpark**. It reads web server logs, filters HTTP error responses, aggregates error counts by endpoint and status code, and writes the results as partitioned Apache Parquet files.

## Pipeline

```text
data/raw_logs.csv
       |
       v
Read CSV -> Filter status_code >= 400 -> Group by endpoint and status_code
                                                  |
                                                  v
                                  output/processed_logs/ (Parquet)
```

`transform_logs` contains the DataFrame transformations, while `process_logs` handles reading and writing data. The processor prints query plans and a preview of the aggregated results before writing output partitioned by `status_code`.

## Project structure

```text
.
├── data/
│   └── raw_logs.csv
├── output/
│   └── processed_logs/
├── src/
│   ├── genrate_data.py
│   └── log_processor.py
├── tests/
│   ├── test_log_processors.py
│   └── test_seesion.py
├── pytest.ini
├── requirements.txt
└── README.md
```

## Getting started

### Prerequisites

- Python 3.8 or later
- A Java runtime compatible with the installed PySpark version

### Install dependencies

From the project root, create and activate a virtual environment, then install the project dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Generate sample data

The generator writes 1,000 sample log records to `data/raw_logs.csv`. Run it from the project root:

```bash
python3 src/genrate_data.py
```

### Run the processor

```bash
python3 src/log_processor.py
```

The processor reads `data/raw_logs.csv` and overwrites `output/processed_logs/` with Parquet output partitioned by status code.

## Run tests

Run the log processor test from the project root:

```bash
pytest tests/test_log_processors.py -v
```

`pytest.ini` adds the project root to pytest's Python import path so the test can import `src.log_processor`. The test uses a local Spark session to check that a `/login` 404 is counted correctly and successful responses are filtered out.

## Local Spark troubleshooting

The processor and test fixture bind the Spark driver to `127.0.0.1`. This can help avoid local hostname resolution errors when running Spark on macOS.

## Possible future enhancements

- Add a GitHub Actions workflow to run tests on pull requests.
- Support reading compressed JSON or GZIP log data from cloud storage.
- Add schema enforcement and data quality checks before writing output.
