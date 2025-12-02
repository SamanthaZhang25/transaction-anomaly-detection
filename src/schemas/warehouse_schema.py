# PySpark schema definitions for warehouse tables
from pyspark.sql.types import *
from src.utils.config_loader import load_schema

def get_fact_transactions_schema():
    """Schema for fact_transactions."""
    schema_dict = load_schema()["fact_transactions"]
    # Define StructType based on schema_dict
    fields = [
        StructField("tx_id", StringType(), False),
        StructField("user_id", StringType(), False),
        # Add all fields...
    ]
    return StructType(fields)

# Similar functions for other tables
