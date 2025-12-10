from confluent_kafka import Consumer
import json

# Set up Kafka Consumer
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'fraud-detector',
    'auto.offset.reset': 'latest'
})

consumer.subscribe(['enriched_transactions'])

print("Listening for transactions on 'enriched_transactions'...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
            continue
        try:
            txn = json.loads(msg.value().decode('utf-8'))
            amount = float(txn.get("amount", 0))
            user = txn.get("user_id", "unknown")
            ip = txn.get("ip", "N/A")
            country = txn.get("country", "N/A")

            if amount > 100000:
                print(f"[FRAUD] ₹{amount} | User: {user} | IP: {ip} | Country: {country}")
            else:
                print(f"[OK] ₹{amount} | User: {user} | IP: {ip} | Country: {country}")

        except Exception as e:
            print("Error parsing transaction:", e)

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()
