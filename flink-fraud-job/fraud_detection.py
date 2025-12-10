import os
import time
import json
import joblib
import urllib.request
import pandas as pd  # ✅ Added for DataFrame input
from pyflink.common import SimpleStringSchema, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer

MODEL_PATH = os.getenv("MODEL_PATH", "/tmp/model.joblib")
DAGSHUB_URL = os.getenv(
    "MODEL_URL",
    "https://dagshub.com/sarveshchezhian2003/Fraud-Detection-System/raw/main/5-MLFlow_Training/model.joblib"
)

def download_model():
    if not os.path.exists(MODEL_PATH):
        print(f"[INFO] Downloading model from DagsHub: {DAGSHUB_URL}")
        try:
            urllib.request.urlretrieve(DAGSHUB_URL, MODEL_PATH)
            print("[INFO] Model downloaded successfully.")
        except Exception as e:
            raise RuntimeError(f"Failed to download model: {e}")
    else:
        print(f"[INFO] Model already exists at {MODEL_PATH}. Skipping download.")

def load_model():
    print(f"[INFO] Loading model from: {MODEL_PATH}")
    return joblib.load(MODEL_PATH)

class FraudDetector:
    def __init__(self):
        self.model = load_model()
        self.last_loaded = os.path.getmtime(MODEL_PATH)

    def maybe_reload_model(self):
        current_mtime = os.path.getmtime(MODEL_PATH)
        if current_mtime != self.last_loaded:
            print("[INFO] Detected updated model. Reloading...")
            self.model = load_model()
            self.last_loaded = current_mtime

    def predict(self, transaction_json):
        try:
            self.maybe_reload_model()

            txn = json.loads(transaction_json)
            txn_id = txn.get("transaction_id")
            user = txn.get("user_id")
            amount = float(txn.get("amount", 0))
            tx_type = txn.get("type", "")
            ip = txn.get("ip_address", "")
            city = txn.get("city", "")
            country = txn.get("country", "")

            features = pd.DataFrame([{
                "amount": amount,
                "type_encoded": 1.0 if tx_type == "TRANSFER" else 0.0,
                "delta_orig": float(txn.get("delta_orig", 0)),
                "delta_dest": float(txn.get("delta_dest", 0))
            }])

            prediction = self.model.predict(features)[0]
            is_fraud = bool(prediction)

            if is_fraud:
                return f"[ML FRAUD ALERT] {txn_id} | User: {user} | ₹{amount} | IP: {ip} ({city}, {country})"
            return None

        except Exception as e:
            return f"[ERROR] {str(e)}"

def main():
    download_model()

    env = StreamExecutionEnvironment.get_execution_environment()

    kafka_props = {
        'bootstrap.servers': 'host.docker.internal:29092',
        'group.id': 'fraud-detector-group',
        'auto.offset.reset': 'earliest'
    }

    consumer = FlinkKafkaConsumer(
        topics='enriched_transactions',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    stream = env.add_source(consumer)

    detector = FraudDetector()

    def process(txn_json):
        return detector.predict(txn_json)

    stream.map(process, output_type=Types.STRING()) \
          .filter(lambda x: x is not None) \
          .print()

    env.execute("FraudDetectionJob")

if __name__ == '__main__':
    main()