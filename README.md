# NYC Taxi Medallion Data Engineering Project (Databricks)

Production-ready Data Engineering project built with PySpark + Delta Lake on Databricks using Medallion Architecture:

- Bronze: raw ingest + schema enforcement + audit columns
- Silver: cleansing + type handling + enrichment + quality checks
- Gold: analytical aggregations for BI and reporting

## Project Structure

```text
.
├── config/
│   ├── databricks_config.py
│   └── schema_config.py
├── src/
│   ├── ingestion/read_data.py
│   ├── transformation/transform_data.py
│   └── write/write_data.py
├── pipeline/
│   └── etl_pipeline.py
├── tests/
│   └── test_transform_data.py
├── databricks.yml
└── .github/workflows/deploy.yml
```

## How It Works

1. Pipeline reads NYC Taxi input from `--source-path` in CSV or Parquet format.
2. Bronze data is written as Delta table with:
   - `ingestion_time`
   - `source_file`
3. Silver processing removes duplicates, handles nulls, casts key fields, and adds:
   - `trip_duration_minutes`
   - `pickup_date`
   - `pickup_hour`
   - formatted timestamps
4. Gold produces:
   - trips by pickup zone
   - trips by hour
   - distance-band aggregations

## Databricks Asset Bundle

`databricks.yml` includes:

- Job resource using `spark_python_task`
- Serverless execution (no `new_cluster` defined)
- `performance_target: STANDARD`
- Daily schedule at `6:00 AM` IST (`Asia/Kolkata`)
- `dev` and `prod` deployment targets

## CI/CD Deployment

GitHub Actions workflow `.github/workflows/deploy.yml`:

- Triggers on push to `main`
- Installs Databricks CLI
- Validates bundle
- Deploys bundle to Databricks

Required GitHub secrets:

- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`

## Local Test

Install dependencies:

```bash
pip install -r requirements.txt
```

Run tests:

```bash
pytest -q
```

## Runtime Parameters

Pipeline accepts optional args:

- `--catalog`
- `--schema`
- `--source-path`
- `--source-format`
- `--bronze-table`
- `--silver-table`
- `--gold-zone-table`
- `--gold-hour-table`
- `--gold-distance-table`
- `--checkpoint-root`

These are already wired into `databricks.yml` variables for reusable deployments.
