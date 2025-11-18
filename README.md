# Synthetic Transaction Anomaly Detection System

## Overview
An end-to-end anomaly detection pipeline designed to identify suspicious financial transactions indicative of money laundering patterns. This system leverages machine learning (Isolation Forest, Autoencoders, LSTM) and provides explainable AI outputs (SHAP) to assist compliance officers in reviewing flagged transactions.

## Key Features
- **Multi-Algorithm Detection**: Combines Isolation Forest, Autoencoders, and LSTMs for robust anomaly detection.
- **Explainable AI**: Integrated SHAP values to explain why a transaction was flagged.
- **Synthetic Data Generation**: Custom generator for realistic transaction data with injected money laundering typologies.
- **MLOps Integration**: MLflow for experiment tracking and Evidently AI for model monitoring.

## Project Structure
- `data/`: Raw and processed transaction data.
- `notebooks/`: Jupyter notebooks for EDA and model experimentation.
- `src/`: Source code for data generation, models, and API.
- `configs/`: Configuration files for model hyperparameters.

## Getting Started

### Prerequisites
- Python 3.10+
- Docker (optional for deployment)

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/SamanthaZhang25/transaction-anomaly-detection.git
   cd transaction-anomaly-detection
   ```
2. Create and activate a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Usage
1. **Generate Data**: Run the data generation script to create synthetic transactions.
   ```bash
   python src/data_generator.py
   ```
2. **Train Model**: Train the anomaly detection models.
   ```bash
   python src/train.py
   ```
3. **Run API**: Start the FastAPI server for real-time scoring.
   ```bash
   uvicorn src.api.main:app --reload
   ```
