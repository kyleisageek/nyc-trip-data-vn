NYC Taxi data is often referenced in data analytics projects

   .gitignore              # Ignores .env, *.parquet, tmp/, __pycache__/
    requirements.txt        # boto3, requests, pyarrow, pyiceberg, pyyaml, python-dotenv
    config.yaml             # Pipeline config with ${ENV_VAR} placeholders
    .env.example            # Documents required env vars
    scripts/
      __init__.py
      schemas.py             # PyArrow schemas for yellow, green, fhvhv (from actual data)
      config.py              # YAML + env var resolution + dotenv loading
      discover.py            # Concurrent HEAD-based URL probing
      r2_client.py           # boto3 S3-compatible client with multipart upload
      iceberg_register.py    # PyIceberg REST catalog with batched 500k-row append
      ingest.py              # Main orchestrator with --dry-run and --types flags

  How to use:
  - python -m scripts.ingest --dry-run — discover available files without downloading
  - python -m scripts.ingest --dry-run --types green — only check green taxi data
  - python -m scripts.ingest — full pipeline (requires .env with R2 credentials)
  - python -m scripts.ingest --types green — process only green taxi data
