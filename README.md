# Risk Control Data Warehouse & ETL Pipeline

## Streaming Pipeline
Run micro-batch processing:
```bash
python3 src/ingestion/streaming_etl.py
```
Simulates real-time ingestion from `data/raw/stream/`, processes with features/quality/scoring, appends to streaming Parquet tables.
