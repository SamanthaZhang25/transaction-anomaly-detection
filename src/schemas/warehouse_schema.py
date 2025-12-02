from pyspark.sql.types import *

def get_fact_transactions_schema():
    return StructType([
        StructField('tx_id', StringType(), False),
        StructField('user_id', StringType(), False),
        StructField('merchant_id', StringType(), False),
        StructField('device_id', StringType(), False),
        StructField('event_time', TimestampType(), False),
        StructField('event_date', DateType(), False),
        StructField('account_country', StringType(), True),
        StructField('ip_country', StringType(), True),
        StructField('ship_country', StringType(), True),
        StructField('amount', DoubleType(), False),
        StructField('currency', StringType(), True),
        StructField('amount_gbp', DoubleType(), True),
        StructField('payment_method', StringType(), True),
        StructField('channel', StringType(), True),
        StructField('product_type', StringType(), True),
        StructField('is_cross_border', BooleanType(), False),
        StructField('fx_rate_source', StringType(), True)
    ])

def get_fact_risk_events_schema():
    return StructType([
        StructField('risk_event_id', StringType(), False),
        StructField('tx_id', StringType(), True),
        StructField('user_id', StringType(), True),
        StructField('merchant_id', StringType(), True),
        StructField('risk_type', StringType(), False),
        StructField('source_system', StringType(), False),
        StructField('created_at', TimestampType(), False)
    ])

def get_dim_user_schema():
    return StructType([
        StructField('user_id', StringType(), False),
        StructField('kyc_level', StringType(), True),
        StructField('registration_country', StringType(), True),
        StructField('user_created_at', TimestampType(), True),
        StructField('has_pep_hit', BooleanType(), False),
        StructField('has_sanctions_hit', BooleanType(), False),
        StructField('user_risk_tier', StringType(), True)
    ])

def get_dim_merchant_schema():
    return StructType([
        StructField('merchant_id', StringType(), False),
        StructField('merchant_country', StringType(), True),
        StructField('merchant_category', StringType(), True),
        StructField('merchant_risk_tier', StringType(), True)
    ])

def get_dim_device_schema():
    return StructType([
        StructField('device_id', StringType(), False),
        StructField('first_seen_at', TimestampType(), True),
        StructField('device_type', StringType(), True),
        StructField('os', StringType(), True),
        StructField('is_emulator_flag', BooleanType(), False)
    ])

def get_dim_country_schema():
    return StructType([
        StructField('country_code', StringType(), False),
        StructField('is_high_risk_country', BooleanType(), False),
        StructField('region', StringType(), True)
    ])

def get_dim_payment_method_schema():
    return StructType([
        StructField('payment_method', StringType(), False),
        StructField('payment_type', StringType(), True),
        StructField('is_high_risk_method', BooleanType(), False)
    ])
