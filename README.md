Designed and implemented a real-time fraud detection pipeline leveraging Kafka and Go. Built a custom Kafka producer in Go to simulate financial transactions and publish events to Kafka topics. Developed an enrichment agent using Go, integrating GeoIP lookups via MaxMind DB and Redis caching to enhance transaction data before forwarding it to downstream Kafka topics. Created a rule-based fraud detection engine to flag suspicious activity, with plans to extend the system using Apache Flink for streaming analytics, integrate alerting mechanisms, and deploy real-time monitoring with Grafana dashboards.

Libraries:
    dvc
    dagshub
    mlflow==2.22.1


NOTE: 
- In Airflow, follow this order
    1. Pull DVC data
    2. Execute python train.py
def pull_dvc_data():
    subprocess.run(["dvc", "pull"], check=True)
