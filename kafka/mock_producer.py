import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

# Configuration matching your docker-compose setup
BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'ecommerce-events'

def create_kafka_producer():
    """Initializes the connection to the local Kafka broker."""
    try:
        return KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5
        )
    except Exception as e:
        print(f"❌ Failed to connect to Kafka at {BOOTSTRAP_SERVERS}: {e}")
        print("💡 Ensure your Docker containers are running using 'docker-compose up -d'")
        exit(1)

def generate_mock_event():
    """Generates a realistic e-commerce user event payload."""
    user_id = f"USR_{random.randint(1000, 9999)}"
    product_id = f"PROD_{random.randint(100, 500)}"
    event_type = random.choices(
        ['view_item', 'add_to_cart', 'purchase'], 
        weights=[0.70, 0.20, 0.10], 
        k=1
    )[0]
    
    # Simulate a data validation edge case (sometimes missing price)
    price = round(random.uniform(5.99, 149.99), 2) if event_type != 'view_item' else None

    return {
        "event_id": f"evt_{random.randint(100000, 999999)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": user_id,
        "event_type": event_type,
        "product_id": product_id,
        "price": price,
        "device": random.choice(['mobile', 'desktop', 'tablet'])
    }

def main():
    print(f"🚀 Starting live event simulator. Streaming to Kafka topic: '{TOPIC_NAME}'...")
    producer = create_kafka_producer()
    
    try:
        while True:
            event = generate_mock_event()
            
            # Send event asynchronously to Kafka
            producer.send(TOPIC_NAME, value=event)
            print(f"📡 Sent event: {event['event_id']} | {event['event_type']} -> {event['product_id']}")
            
            # Sleep a random fraction of a second to mimic real-world traffic spike behavior
            time.sleep(random.uniform(0.2, 1.5))
            
    except KeyboardInterrupt:
        print("\n🛑 Simulator stopped by user. Flushing remaining messages...")
    finally:
        producer.flush()
        producer.close()
        print("🔌 Kafka connection safely closed.")

if __name__ == "__main__":
    main()
