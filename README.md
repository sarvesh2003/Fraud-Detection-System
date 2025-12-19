# NOTE:

This is Version 1. Version 2 with synthetic data generation (SDV/Gaussian Copula) and gRPC ingestion is available here: [GitHub](https://github.com/sarvesh2003/Real_Time_Financial_Fraud_Detection_System_v2)

# Real-Time Financial Fraud Detection using Kafka, Flink & MLOps Tools
- A production-grade MLOps platform that detects financial fraud in real-time. This system integrates a Golang high-throughput ingestion layer with an Apache Flink ML inference engine, orchestrated by Airflow for continuous model training and deployment.
- The system implements a Lambda Architecture variant where real-time streams are processed against dynamically updated Machine Learning models.

## Activity Diagram
The system follows a Lambda Architecture variant:

1. Go Simulator generates synthetic transactions and pushes to Kafka
2. Go Enricher consumes raw transactions, enriches with GeoIP (Redis-cached), publishes to enriched topic
3. Flink Job consumes enriched stream, runs ML inference, outputs fraud alerts
4. Airflow Pipeline periodically retrains models, which Flink hot-reloads without restart

![alt text](https://github.com/sarvesh2003/Fraud-Detection-System/blob/main/activity_diagram.png)


## Key Features

### High-Throughput Ingestion (Go + Kafka)
- **Concurrent producers**: Multi-goroutine simulation generates realistic financial traffic using WaitGroups
- **Reliable delivery**: Kafka-backed pipelines with configurable partitioning
- **Realistic data**: Transactions include amount, type, balance deltas, IP addresses, and timestamps

### Real-Time Feature Enrichment (Go + Redis + GeoIP)
- **GeoIP augmentation**: Transactions enriched with city/country using MaxMind database
- **Redis look-aside cache**: 24-hour TTL caching eliminates redundant GeoIP lookups
- **Stateful processing**: Consistent data quality under high throughput

### Streaming ML Inference (Apache Flink)
- **Sub-second predictions**: Random Forest classifier serves predictions on live streams
- **Hot-swap model reloading**: File-timestamp monitoring enables zero-downtime updates
- **Automatic model fetching**: Downloads latest model from DAGsHub registry on startup

### Continuous Training Pipeline (Airflow + DVC + MLflow)
- **Scheduled retraining**: Airflow DAGs trigger every 5 minutes (configurable)
- **Data versioning**: DVC pulls latest training data and tracks lineage
- **Experiment tracking**: Metrics (AUC, Accuracy) and parameters logged to MLflow
- **Seamless deployment**: New `model.joblib` overwrites trigger Flink hot-reload

## Technology Stack

| Layer | Technologies |
|-------|--------------|
| **Ingestion** | Go, Sarama (Kafka Client) |
| **Streaming** | Apache Kafka, Zookeeper |
| **Enrichment** | Go, Redis, MaxMind GeoIP |
| **Processing** | Apache Flink (PyFlink), Pandas |
| **ML** | Scikit-Learn (Random Forest), Joblib |
| **MLOps** | Apache Airflow, MLflow, DVC, DAGsHub |
| **Infrastructure** | Docker, Docker Compose |

## ML Model Details

| Attribute | Value |
|-----------|-------|
| Algorithm | Random Forest Classifier |
| Features | `amount`, `type_encoded`, `delta_orig`, `delta_dest` |
| Tracking | MLflow (metrics: AUC, Accuracy) |
| Serving | PyFlink with hot-reload capability |

## Getting Started

### Prerequisites
- Docker & Docker Compose
- Go 1.19+
- Python 3.9+

### Quick Start
```bash
# Clone the repository
git clone https://github.com/yourusername/fraud-detection-v1.git
cd fraud-detection-v1

# Start infrastructure (Kafka, Redis, Zookeeper)

# Run the Go simulator
cd simulator && go run main.go

# Run the Go enricher
cd enricher && go run main.go

# Start the Flink inference job
cd flink && python fraud_detection.py
```

## Project Structure

```
├── simulator/           # Go concurrent transaction producers
├── enricher/            # Go service with Redis + GeoIP
├── flink/               # PyFlink streaming inference job
├── training/            # Model training scripts + MLflow
├── airflow/             # DAGs for continuous training
├── docker-compose.yml   # Infrastructure setup
└── README.md
```

## What's in Version 2?

- **Synthetic Data Generation**: SDV (Gaussian Copula) trained on Databricks FSI Fraud dataset
- **gRPC Integration**: Python → Go cross-language communication
- **Enhanced Realism**: Statistically accurate transaction distributions

➡️ [Check out Version 2](https://github.com/sarvesh2003/Real_Time_Financial_Fraud_Detection_System_v2)



