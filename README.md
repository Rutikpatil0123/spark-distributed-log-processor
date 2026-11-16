# Distributed Log Processing with Apache Spark

A small PySpark project that processes web server logs locally. It filters HTTP error responses, counts errors for each endpoint and status code, and writes the aggregated results to partitioned Parquet files.

## What it does

The pipeline reads a CSV file, keeps rows where `status_code >= 400`, groups the remaining events by `endpoint` and `status_code`, and counts them as `error_count`.

```text
data/raw_logs.csv
       |
       v
Read CSV -> Filter HTTP errors -> Aggregate endpoint + status code
                                      |
                                      v
                         output/processed_logs/
                         Parquet partitioned by status_code
```

The transformation is implemented in `transform_logs`; `process_logs` reads the input and writes the output. The processor also prints Spark query plans and a preview of the aggregation.

## Project structure

```text
.
├── data/
│   └── raw_logs.csv
├── output/
│   └── processed_logs/       # Generated; excluded from Git
├── src/
│   ├── genrate_data.py       # Generates sample CSV data
│   └── log_processor.py      # Spark transformations and pipeline
├── tests/
│   ├── test_log_processors.py
│   └── test_seesion.py
├── pytest.ini                # Adds the project root to pytest's import path
├── requirements.txt
└── README.md
```

## Requirements

- Python
- Java runtime compatible with the installed PySpark version

`requirements.txt` contains PySpark and pytest. Install them in a virtual environment:

```bash
git clone https://github.com/Rutikpatil0123/spark-distributed-log-processor.git
cd spark-distributed-log-processor

python3 -m venv venv
source venv/bin/activate
python -m pip install -r requirements.txt
```

On Windows PowerShell, activate the environment with:

```powershell
.\venv\Scripts\Activate.ps1
```

## Run the pipeline

Commands should be run from the project root because the scripts use paths relative to that directory.

To generate 1,000 sample log records (this overwrites `data/raw_logs.csv`):

```bash
python src/genrate_data.py
```

To process the CSV:

```bash
python src/log_processor.py
```

The processor reads `data/raw_logs.csv` and overwrites `output/processed_logs/`. The output is partitioned into directories such as `status_code=404/`; each Parquet record contains `endpoint`, `status_code`, and `error_count`.

## Input data

The CSV has a header row and these columns:

| Column | Description |
| --- | --- |
| `ip_address` | Client IP address |
| `timestamp` | Time of the request |
| `endpoint` | Requested URL path |
| `status_code` | HTTP response status code |

The bundled CSV is sample data. The generator chooses IPs, endpoints, timestamps, and status codes randomly, so regenerated data and aggregation counts will vary.

## Run tests

Run the aggregation test:

```bash
pytest tests/test_log_processors.py -v
```

Or run all discovered tests, including the Spark-session smoke test:

```bash
pytest -q
```

`pytest.ini` configures the repository root on the Python import path so tests can import `src.log_processor`. The aggregation test checks that successful responses are excluded and that repeated `/login` 404 responses are counted correctly.

## Local Spark notes

The pipeline and test fixture bind the Spark driver to `127.0.0.1`, which can help avoid local hostname-resolution issues on macOS. PySpark also requires a compatible Java runtime; consult the compatibility notes for the PySpark version installed in your environment if Spark cannot start.
