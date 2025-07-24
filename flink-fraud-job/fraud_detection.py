from pyflink.common import SimpleStringSchema, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
import json

def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    kafka_props = {
        'bootstrap.servers': 'host.docker.internal:29092',  # Update here
        'group.id': 'fraud-detector-group',
        'auto.offset.reset': 'earliest'
    }

    consumer = FlinkKafkaConsumer(
        topics='enriched_transactions',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    stream = env.add_source(consumer)

    def process(transaction_json):
        try:
            txn = json.loads(transaction_json)
            amount = float(txn.get("amount", 0))
            if amount > 10:
                return f"[FRAUD ALERT] Transaction {txn['transaction_id']} by user {txn['user_id']} for ₹{amount} from {txn['ip_address']} ({txn.get('city', '')}, {txn.get('country', '')})"
            return None
        except Exception as e:
            return f"[ERROR] Invalid input: {str(e)}"

    stream.map(process, output_type=Types.STRING()).filter(lambda x: x is not None).print()

    env.execute("FraudDetectionJob")

if __name__ == '__main__':
    main()
