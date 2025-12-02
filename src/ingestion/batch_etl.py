# Batch ETL Pipeline for Risk Control Data Warehouse

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.utils.io_utils import create_spark_session, get_paths
from src.utils.config_loader import load_config
from src.schemas.warehouse_schema import *
import yaml


def main():
    spark = create_spark_session()
    config = load_config('config/settings.yml')
    paths = get_paths()

    # Load raw data with schemas
    df_transactions = spark.read.option('header', 'true').csv(paths['raw_data_path'] + '/transactions.csv', schema=get_fact_transactions_schema())
    df_users = spark.read.option('header', 'true').csv(paths['raw_data_path'] + '/users.csv', schema=get_dim_user_schema())
    df_merchants = spark.read.option('header', 'true').csv(paths['raw_data_path'] + '/merchants.csv', schema=get_dim_merchant_schema())
    df_devices = spark.read.option('header', 'true').csv(paths['raw_data_path'] + '/devices.csv', schema=get_dim_device_schema())
    df_kyc_flags = spark.read.option('header', 'true').csv(paths['raw_data_path'] + '/kyc_flags.csv')
    df_alerts = spark.read.option('header', 'true').csv(paths['raw_data_path'] + '/alerts.csv')

    # Clean and normalize transactions
    df_transactions = df_transactions \
        .withColumn('event_date', to_date('event_time')) \
        .withColumn('amount_gbp', when(col('currency') == 'USD', col('amount') * lit(config['fx_rates']['USD'])) \
                             .when(col('currency') == 'EUR', col('amount') * lit(config['fx_rates']['EUR'])) \
                             .otherwise(col('amount'))) \
        .withColumn('is_cross_border', col('account_country') != col('ship_country')) \
        .na.fill({'amount_gbp': 0.0})

    # Deduplicate and filter
    df_transactions = df_transactions.dropDuplicates(['tx_id'])
    df_transactions = df_transactions.filter(col('amount') > 0)

    # Write fact table partitioned
    df_transactions.write.mode('overwrite').partitionBy('event_date', 'account_country').parquet(paths['parquet_output'] + '/fact_transactions')

    # Dimension tables (simple dedup and write)
    df_users = df_users.dropDuplicates(['user_id']).withColumn('user_created_at', to_timestamp('user_created_at'))
    df_users.write.mode('overwrite').parquet(paths['parquet_output'] + '/dim_user')

    df_merchants = df_merchants.dropDuplicates(['merchant_id'])
    df_merchants.write.mode('overwrite').parquet(paths['parquet_output'] + '/dim_merchant')

    df_devices = df_devices.dropDuplicates(['device_id']).withColumn('first_seen_at', to_timestamp('first_seen_at'))
    df_devices.write.mode('overwrite').parquet(paths['parquet_output'] + '/dim_device')

    # Create dim_country from config
    high_risk_countries = spark.createDataFrame([(c, True, 'High Risk') for c in config['high_risk_countries']] + [('US', False, 'North America'), ('GB', False, 'Europe')], ['country_code', 'is_high_risk_country', 'region'])
    high_risk_countries.write.mode('overwrite').parquet(paths['parquet_output'] + '/dim_country')

    df_payment_methods = spark.createDataFrame([('card', 'card', False), ('wallet', 'wallet', True), ('bank', 'bank', False)], ['payment_method', 'payment_type', 'is_high_risk_method'])
    df_payment_methods.write.mode('overwrite').parquet(paths['parquet_output'] + '/dim_payment_method')

    # Risk events from alerts and kyc_flags
    df_risk_events = df_alerts.selectExpr('alert_id as risk_event_id', 'user_id', 'alert_type as risk_type', 'severity as source_system', 'created_at')
    df_risk_events = df_risk_events.union(df_kyc_flags.selectExpr('user_id', 'kyc_flag as risk_type', 'status as source_system', current_timestamp().alias('created_at')))
    df_risk_events.write.mode('overwrite').parquet(paths['parquet_output'] + '/fact_risk_events')

    # Call feature engineering
    from src.features.risk_features import engineer_features
    df_enhanced = engineer_features(df_transactions)
    df_enhanced.write.mode('overwrite').partitionBy('event_date').parquet(paths['parquet_output'] + '/fact_transactions_enhanced')

    # Quality checks
    from src.quality.data_quality_checks import run_quality_checks
    quality_report = run_quality_checks(df_transactions, config)
    print(quality_report)

    # Risk scoring
    from src.scoring.risk_scoring import score_risk
    df_scored = score_risk(df_enhanced)
    df_scored.write.mode('overwrite').parquet(paths['parquet_output'] + '/risk_alerts')

    spark.stop()
    print('ETL pipeline completed successfully!')

if __name__ == '__main__':
    main()
