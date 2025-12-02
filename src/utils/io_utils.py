# Utility functions for I/O operations

from pyspark.sql import SparkSession
import yaml
from pathlib import Path

def create_spark_session(config_path: str = "config/settings.yml") -> SparkSession:
    """Create and configure SparkSession."""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    spark = SparkSession.builder \
        .appName(config["spark"]["app_name"]) \
        .master(config["spark"]["master"]) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    return spark

def get_paths(config_path: str = "config/settings.yml") -> dict:
    """Load paths from config."""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config["paths"]
