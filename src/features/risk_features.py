# Risk Feature Engineering

from pyspark.sql.functions import *

def engineer_features(df):
    """Engineer risk features."""
    # Example: velocity, amount percentiles
    df = df.withColumn("amount_velocity", ...)  # Placeholder
    return df
