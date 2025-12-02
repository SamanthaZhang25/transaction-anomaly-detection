# Batch ETL Pipeline

from pyspark.sql import SparkSession
import pandas as pd
from src.utils.io_utils import create_spark_session, get_paths
from src.utils.config_loader import load_config
from src.schemas.warehouse_schema import get_fact_transactions_schema

def main():
    spark = create_spark_session()
    config = load_config()
    paths = get_paths()
    
    # Read raw data
    df_transactions = spark.read.csv(paths["raw_data_path"] + "/transactions.csv", header=True, schema=get_fact_transactions_schema())
    
    # Clean and transform (placeholder)
    df_clean = df_transactions.na.fill(0)  # Example
    
    # Write to warehouse
    df_clean.write.mode("overwrite").parquet(paths["parquet_output"] + "/fact_transactions")
    
    # Call feature engineering, quality checks
    spark.stop()

if __name__ == "__main__":
    main()
