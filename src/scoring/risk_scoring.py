# Risk Scoring and Anomaly Detection

from pyspark.sql.functions import *

def score_risk(df):
    """Apply rules and anomaly detection."""
    # Rule-based scoring
    df = df.withColumn("risk_score", when(col("amount") > 10000, 0.8).otherwise(0.2))
    # Write to risk_alerts
    return df
