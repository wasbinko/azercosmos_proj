# Azercosmos Telemetry ML Pipeline

An end-to-end Machine Learning and monitoring pipeline for processing, modeling, and alerting on telemetry data. This application utilizes Docker for containerization (Kafka), MLflow for model tracking, Streamlit for an interactive dashboard, and a dedicated Python daemon for proactive email alerts.

## 📑 Table of Contents
- [Project Structure](#project-structure)[cite: 2]
- [Core Components](#core-components)[cite: 2]
- [Prerequisites](#prerequisites)[cite: 2]
- [Getting Started](#getting-started)[cite: 2]
- [Usage Guide](#usage-guide)[cite: 2]
- [Monitoring & Diagnostics](#monitoring--diagnostics)[cite: 2]

---

## 🏗️ Project Structure

```text
azercosmos_proj/
├── alert_daemon.py          # Background service for triggering email alerts
├── app/
│   └── app.py               # Streamlit application for real-time dashboard and deep dives
├── docker-compose.yml       # Docker orchestration for Kafka (KRaft mode)
├── generate_telemetry.py    # Script to simulate or ingest raw telemetry data with anomalies
├── requirements.txt         # Python dependencies
└── scripts/                 # ML and Data processing scripts
    ├── check_drift.py       # Detects data drift in telemetry against training baseline
    ├── diagnose_stat.py     # Statistical diagnostics for the StatDetector
    ├── drift.py             # Core Population Stability Index (PSI) drift calculation logic
    ├── explain.py           # Model explainability (SHAP & Captum) for XGBoost/Forecasters
    ├── infer.py             # Runs model inference and scoring on incoming data
    ├── kafka_io.py          # Handles Kafka producers/consumers for data streams
    ├── models.py            # ML model definitions (LSTM, PatchTST, XGBoost, IF, StatDetector)
    ├── nhits_model.py       # N-HiTS time-series forecasting model (via Darts)
    └── train.py             # Model training and MLflow logging pipeline
