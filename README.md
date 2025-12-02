# Risk Control Data Warehouse & ETL Pipeline

This repository is a self-initiated **risk control data warehouse & ETL demo** for a cross-border e-commerce platform, inspired by JD.com / Joybuy–style use cases.

The goal is to show how a Risk Control Data Engineer can:
- design a **star-schema risk data warehouse**,
- build **batch and (simulated) streaming ETL pipelines** in Python / PySpark,
- engineer **risk features**,
- run **data quality checks**, and
- output a **prioritised risk alerts table** for fraud / risk review.

It is designed to run on a single laptop (local PySpark, local Parquet files).

---

## 1. Architecture overview

At a high level, the project does the following:

1. **Raw data simulation**  
   Synthetic CSV data under `data/raw/` simulate:
   - users and merchants,
   - devices,
   - cross-border transactions in multiple currencies,
   - KYC / risk flags and historical alerts.

2. **Risk data warehouse**  
   A small **star schema** is built on top of this data:
   - `fact_transactions` and `fact_risk_events`
   - `dim_user`, `dim_merchant`, `dim_device`, `dim_country`, `dim_payment_method`

3. **Batch ETL (offline)**  
   A batch pipeline reads raw CSVs, cleans & normalises the data, engineers risk features, runs data quality checks, and writes **partitioned Parquet tables** under `data/warehouse/`.

4. **Streaming ETL (micro-batch simulation)**  
   A simple streaming job reads new transaction files from `data/raw/stream/` in micro-batches, applies a lighter version of the risk feature and scoring logic, and appends to streaming Parquet tables.

5. **Risk scoring & alerts**  
   Rule-based and simple anomaly-based scoring produce a `risk_alerts` table with:
   - key transaction/user/merchant fields,
   - engineered risk features,
   - a combined `risk_score`, `risk_level`,
   - and a short `alert_reason`.

This is not a production system, but a **teaching / interview demo** to explain how risk data pipelines could be structured.

---

## 2. Data model (warehouse schema)

The warehouse follows a simplified star schema:

### Fact tables

- **`fact_transactions`**
  - `tx_id`, `user_id`, `merchant_id`, `device_id`
  - `event_time`, `event_date`
  - `account_country`, `ip_country`, `ship_country`
  - `amount`, `currency`, `amount_gbp`
  - `payment_method`, `channel`, `product_type`
  - `is_cross_border`, `fx_rate_source`

- **`fact_risk_events`**
  - `risk_event_id`, `tx_id`, `user_id`, `merchant_id`
  - `risk_type` (e.g. `"fraud_alert"`, `"kyc_hit"`)
  - `source_system` (e.g. `"rules_engine"`, `"manual_review"`)
  - `created_at`

### Dimension tables

- **`dim_user`**
  - `user_id`, `kyc_level`, `registration_country`, `user_created_at`
  - `has_pep_hit`, `has_sanctions_hit`, `user_risk_tier`

- **`dim_merchant`**
  - `merchant_id`, `merchant_country`, `merchant_category`, `merchant_risk_tier`

- **`dim_device`**
  - `device_id`, `first_seen_at`, `device_type`, `os`, `is_emulator_flag`

- **`dim_country`**
  - `country_code`, `is_high_risk_country`, `region`

- **`dim_payment_method`**
  - `payment_method`, `payment_type` (card, wallet, bank), `is_high_risk_method`

All tables are stored as Parquet under `data/warehouse/`, often partitioned by `event_date` and/or `account_country`.

---

## 3. Risk features & scoring

Risk feature engineering is implemented in `src/features/` and applied mainly to `fact_transactions`. Examples include:

- **Location & network risk**
  - `is_high_risk_ip_country`
  - `is_country_mismatch` between account and IP countries

- **Velocity & pattern risk**
  - `user_tx_count_1h`, `user_tx_count_24h`
  - `is_high_frequency_user`
  - `is_integer_amount_pattern` (round GBP amounts, often used for ML/fraud)

- **Relationship / network risk**
  - `has_link_to_flagged_account` based on historical alerts
  - `shared_device_with_flagged_user`

- **Lifecycle / KYC risk**
  - `account_age_days`
  - `has_unresolved_ticket`
  - `kyc_level`, `user_risk_tier`

- **Transaction-side risk**
  - `is_high_amount` (configurable thresholds)
  - `is_cross_border`
  - `is_high_risk_product_type` (gift cards, virtual top-ups, etc.)
  - `is_high_risk_payment_method`

A simple **rule-based risk score** aggregates these into a numeric score and a **risk level** (`LOW`, `MEDIUM`, `HIGH`).  
In addition, basic **anomaly detection** marks:
- very large transactions (above country-specific percentiles),
- unusually high user velocity compared to normal behaviour.

The output is a `risk_alerts` table used for prioritised review.

---

## 4. Project structure

High-level structure:

```text
config/
  schema.yml              # high-level description of warehouse tables (if present)
  settings.yml            # risk thresholds, high-risk countries, etc. (if present)

data/
  raw/                    # synthetic CSV inputs
  raw/stream/             # streaming-style new transactions
  warehouse/              # Parquet output (fact_*, dim_*, risk_alerts)

notebooks/
  01_explore_raw_data.ipynb
  02_explore_warehouse_and_alerts.ipynb

src/
  ingestion/
    batch_etl.py          # main batch ETL job
    streaming_etl.py      # micro-batch / streaming simulation
  features/
    risk_features.py      # feature engineering logic
  quality/
    data_quality_checks.py# data quality rules & reporting
  scoring/
    risk_scoring.py       # risk scoring & alert generation
  utils/
    io_utils.py           # Spark session / IO helpers
    config_loader.py      # config parsing helpers

requirements.txt
README.md
