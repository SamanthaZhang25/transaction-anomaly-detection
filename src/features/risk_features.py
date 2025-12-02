# Risk Feature Engineering

from pyspark.sql.functions import *
from pyspark.sql.window import Window


def engineer_features(df):
    """Engineer risk features for anomaly detection."""
    # Temporal features
    window_spec = Window.partitionBy('user_id').orderBy('event_time').rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df = df.withColumn('tx_count_last_hour', count('tx_id').over(window_spec)) \
           .withColumn('avg_amount_last_10_tx', avg('amount').over(window_spec.rowsBetween(-9, 0))) \
           .withColumn('velocity_ratio', col('amount') / when(col('avg_amount_last_10_tx') > 0, col('avg_amount_last_10_tx')).otherwise(1))

    # Geographic features
    df = df.withColumn('geo_mismatch', col('account_country') != col('ip_country')) \
           .withColumn('cross_border_flag', col('is_cross_border')) \
           .withColumn('high_risk_country', col('ip_country').isin(['RU', 'IR', 'KP', 'SY']))

    # Device and payment features
    df = df.withColumn('high_risk_payment', col('payment_method').isin(['crypto', 'wire'])) \
           .withColumn('channel_risk', when(col('channel') == 'mobile_app', 0.3).otherwise(0.1))

    # Aggregate features (user-level)
    user_features = df.groupBy('user_id').agg(
        count('tx_id').alias('total_tx'),
        avg('amount').alias('avg_tx_amount'),
        max('amount').alias('max_tx_amount'),
        countDistinct('merchant_id').alias('unique_merchants'),
        countDistinct('device_id').alias('unique_devices')
    )

    # Join back
    df = df.join(user_features, 'user_id', 'left')
    df = df.withColumn('user_velocity', col('total_tx') / (datediff(max('event_time').over(Window.partitionBy('user_id')), min('user_created_at').over(Window.partitionBy('user_id'))) + 1))

    # Risk composite score (simple weighted sum)
    df = df.withColumn('risk_feature_score',
        (col('velocity_ratio') * 0.2) +
        (when(col('geo_mismatch'), 1).otherwise(0) * 0.3) +
        (col('high_risk_country') * 0.4) +
        (col('high_risk_payment') * 0.1)
    )

    return df
