# Real-Time Financial Fraud Detection using Kafka, Flink & MLOps Tools
- A production-grade MLOps platform that detects financial fraud in real-time. This system integrates a Golang high-throughput ingestion layer with an Apache Flink ML inference engine, orchestrated by Airflow for continuous model training and deployment.
- The system implements a Lambda Architecture variant where real-time streams are processed against dynamically updated Machine Learning models.
## Key Features
### Real-Time ML Inference using PyFlink
- **Scikit-Learn Integration:** The Flink job deserializes the transaction stream and passes features (amount, delta_orig, delta_dest) into a pre-trained Random Forest Classifier.
- **Dynamic Model Reloading:** The system implements a Hot-Swap mechanism. The Flink worker monitors the model file timestamp. If the Airflow pipeline pushes a new model, Flink reloads it into memory without stopping the stream.
- **Automated Model Fetching:** On startup, the system automatically downloads the latest production model from the DAGsHub remote registry.
### High-Performance Ingestion using Golang
- **Concurrent Simulation:** A Go-based simulator generates realistic financial traffic patterns.
- **Geo-Enrichment:** A Go microservice enriches transactions with geolocation (City/Country) using in-memory GeoIP lookups and a Redis Look-Aside cache for low latency.
### Automated MLOps using Airflow & DVC
- **Continuous Training (CT):** An Airflow DAG runs on a schedule to pull the latest dataset via DVC, retrain the model, and push the new artifact to the registry.
- **Experiment Tracking:** All training runs, metrics (AUC/Accuracy), and parameters are logged to MLflow for auditability.

## Technology Stack
- **Ingestion:** Golang, Sarama (Kafka Client).
- **Streaming:** Apache Kafka, Zookeeper.
- **Processing:** Apache Flink (PyFlink), Pandas.
- **Machine Learning:** Scikit-Learn, Joblib.
- **MLOps:** Apache Airflow, MLflow, DVC, DAGsHub.
- **Infrastructure:** Docker, Docker Compose, Redis.

## Activity Diagram
![alt text](https://github.com/sarvesh2003/Fraud-Detection-System/blob/main/activity_diagram.png)


