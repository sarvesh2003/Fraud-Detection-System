• Architected a low-latency, event-driven fraud detection pipeline using Apache Kafka and Apache Flink to process and flag suspicious financial transactions in real time.

• Developed a custom Go-based Kafka producer to simulate realistic financial transactions and publish them to Kafka topics for downstream processing.

• Designed and deployed a microservices-based enrichment agent in Go, integrating GeoIP lookups via MaxMind DB and Redis caching to enrich and standardize transaction data with high throughput.

• Implemented a rule-based fraud detection engine as the baseline system, with an extensible design supporting ML-driven anomaly detection models.

• Applied MLOps best practices—integrating MLflow for experiment tracking and model management, DVC and DagsHub for dataset/model versioning, and Apache Airflow for pipeline orchestration and automation.

Libraries:
    dvc
    dagshub
    mlflow==2.22.1
