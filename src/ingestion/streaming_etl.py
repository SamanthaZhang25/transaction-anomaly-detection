# Simulated Streaming ETL with Spark Structured Streaming

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from src.utils.io_utils import get_paths
from src.utils.config_loader import load_config
from src.schemas.warehouse_schema import get_fact_transactions_schema
from src.features.risk_features import engineer_features
from src.quality.data_quality_checks import run_quality_checks
from src.scoring.risk_scoring import score_risk


def main():
    spark = SparkSession.builder \
        .appName("streaming_etl") \
        .config("spark.sql.streaming.checkpointLocation", "checkpoints/streaming") \
        .getOrCreate()
    
    config = load_config()
    paths = get_paths()
    stream_path = paths["stream_input"]
    
    # Read streaming input (file-based micro-batches)
    df_stream = spark.readStream \
        .format("csv") \
        .option("header", "true") \
        .schema(get_fact_transactions_schema()) \
        .load(f"file://{stream_path}")
    
    # Apply cleaning and features (reuse batch logic)
    df_processed = df_stream \
        .withColumn("event_date", to_date("event_time")) \
        .withColumn("amount_gbp", col("amount"))  # Simplified for stream
    
    df_enhanced = engineer_features(df_processed)
    
    # Quality checks (simplified for streaming)
    quality_report = run_quality_checks(df_enhanced, config)
    print(quality_report)
    
    # Risk scoring
    _, df_alerts = score_risk(df_enhanced)
    
    # Write to warehouse (append mode)
    query_tx = df_enhanced.writeStream \
        .format("parquet") \
        .option("path", f"file://{paths['warehouse']}/fact_transactions_stream") \
        .option("checkpointLocation", "checkpoints/tx_stream") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    query_alerts = df_alerts.writeStream \
        .format("parquet") \
        .option("path", f"file://{paths['risk_alerts']}") \
        .option("checkpointLocation", "checkpoints/alerts_stream") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    # Await termination (for demo, run 60s)
    spark.streams.awaitAnyTermination(60)
    
    spark.stop()

if __name__ == "__main__":
    main()
