# Data Quality Checks

from pyspark.sql.functions import *
from pyspark.sql.types import *


def run_quality_checks(df, config):
    """Run comprehensive data quality checks."""
    report = {}

    # 1. Completeness (null checks)
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c + '_nulls') for c in df.columns])
    total_rows = df.count()
    report['null_rates'] = {col: null_counts.collect()[0][col] / total_rows for col in df.columns if 'nulls' in col}

    # 2. Uniqueness (duplicates)
    duplicate_count = df.count() - df.dropDuplicates().count()
    report['duplicates'] = duplicate_count

    # 3. Validity (data types, ranges)
    validity_issues = 0
    # Amount > 0
    validity_issues += df.filter(col('amount') <= 0).count()
    # Event time not in future
    validity_issues += df.filter(col('event_time') > current_timestamp()).count()
    report['validity_issues'] = validity_issues

    # 4. Consistency (cross-field)
    inconsistency = df.filter(col('account_country') == col('ip_country')).count()  # Example
    report['consistency_issues'] = total_rows - inconsistency

    # 5. Timeliness (recent data)
    recent_data = df.filter(col('event_date') >= date_sub(current_date(), config['quality']['max_age_days'])).count()
    report['timeliness'] = recent_data / total_rows

    # Generate summary report
    summary = f"Total rows: {total_rows}\nDuplicates: {duplicate_count} ({duplicate_count/total_rows*100:.2f}%)\nValidity issues: {validity_issues}\n"
    for key, value in report['null_rates'].items():
        summary += f"{key[:-6]} null rate: {value*100:.2f}%\n"

    print(summary)
    return summary
