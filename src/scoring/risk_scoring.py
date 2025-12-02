# Risk Scoring and Anomaly Detection

from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline


def score_risk(df):
    """Apply rule-based and ML-based risk scoring."""

    # Rule-based scoring
    df = df.withColumn('rule_score',
        when(col('amount') > 10000, 0.8)
        .when(col('is_cross_border') & col('high_risk_country'), 0.7)
        .when(col('velocity_ratio') > 3, 0.6)
        .when(col('user_risk_tier') == 'high', 0.5)
        .otherwise(0.2)
    )

    # Anomaly detection features
    assembler = VectorAssembler(inputCols=['amount', 'velocity_ratio', 'geo_mismatch', 'high_risk_payment', 'risk_feature_score'], outputCol='features')
    df_assembled = assembler.transform(df)

    # Simple ML model (placeholder - train on labeled data in production)
    # For demo, use rule-based anomaly flag
    df = df.withColumn('anomaly_flag',
        when(col('rule_score') > 0.5, True).otherwise(False)
    )

    # Final risk score (blend rules and features)
    df = df.withColumn('final_risk_score',
        (col('rule_score') * 0.6) + (col('risk_feature_score') * 0.4)
    )

    # Generate alerts for high-risk
    alerts = df.filter(col('final_risk_score') > 0.7).select(
        monotonically_increasing_id().alias('risk_event_id'),
        'tx_id', 'user_id', 'merchant_id',
        lit('rules_engine').alias('source_system'),
        current_timestamp().alias('created_at'),
        lit('high_risk_transaction').alias('risk_type'),
        'final_risk_score'
    )

    return df, alerts
