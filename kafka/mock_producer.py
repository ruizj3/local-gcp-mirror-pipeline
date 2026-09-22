import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

# Configuration matching your docker-compose setup (override via env var on Render, etc.)
BOOTSTRAP_SERVERS = [os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')]
TOPIC_NAME = 'transactions'

MERCHANT_CATEGORIES = ['grocery', 'restaurant', 'electronics', 'travel', 'jewelry', 'digital_goods', 'gaming']
DEVICES = ['mobile', 'desktop', 'tablet']
PAYMENT_METHODS = ['card', 'wallet', 'bank_transfer']
COUNTRIES = ['US', 'CA', 'GB', 'DE', 'FR', 'MX', 'BR', 'NG', 'RU', 'VN']

# Merchant categories favored by fraudsters vs. typical low-risk spend
HIGH_RISK_CATEGORIES = ['electronics', 'jewelry', 'digital_goods', 'gaming']

# In-memory user profile store so features like "new device" / "distance from home"
# are derived from a consistent history instead of being pure noise.
USER_PROFILES = {}


def get_or_create_user():
    """Returns an existing user profile most of the time, occasionally minting a new one."""
    if USER_PROFILES and random.random() < 0.85:
        user_id = random.choice(list(USER_PROFILES.keys()))
    else:
        user_id = f"USR_{random.randint(10000, 99999)}"
        USER_PROFILES[user_id] = {
            "home_country": random.choice(COUNTRIES),
            "known_devices": {random.choice(DEVICES)},
            "account_age_days": random.randint(1, 2000),
            "last_txn_time": None,
            "recent_txn_times": [],
        }
    return user_id, USER_PROFILES[user_id]


def _base_event(user_id, profile, amount, merchant_category, device, is_new_device,
                 payment_method, ip_country, is_fraud, fraud_scenario):
    now = datetime.now(timezone.utc)
    last_txn_time = profile["last_txn_time"]
    time_since_last = (now - last_txn_time).total_seconds() if last_txn_time else 999_999.0

    profile["recent_txn_times"] = [t for t in profile["recent_txn_times"] if (now - t).total_seconds() < 3600]
    profile["recent_txn_times"].append(now)
    profile["last_txn_time"] = now

    event = {
        "event_id": f"txn_{uuid.uuid4().hex[:12]}",
        "timestamp": now.isoformat(),
        "user_id": user_id,
        "event_type": "purchase",
        "amount": amount,
        "merchant_category": merchant_category,
        "device": device,
        "is_new_device": is_new_device,
        "payment_method": payment_method,
        "country": profile["home_country"],
        "ip_country": ip_country,
        "account_age_days": profile["account_age_days"],
        "time_since_last_txn_seconds": round(time_since_last, 2),
        "txn_count_last_1h": len(profile["recent_txn_times"]),
        "distance_from_home_km": 0.0 if ip_country == profile["home_country"] else round(random.uniform(500, 12000), 1),
        "is_fraud": int(is_fraud),
        "fraud_scenario": fraud_scenario,
    }
    return event


def generate_normal_event():
    """A typical, legitimate purchase from a returning (mostly) user."""
    user_id, profile = get_or_create_user()
    device = random.choice(list(profile["known_devices"]) or DEVICES)
    profile["known_devices"].add(device)
    merchant_category = random.choice(MERCHANT_CATEGORIES)
    amount = round(random.uniform(5.99, 149.99), 2)

    return [_base_event(
        user_id, profile, amount, merchant_category, device,
        is_new_device=False,
        payment_method=random.choice(PAYMENT_METHODS),
        ip_country=profile["home_country"],
        is_fraud=False,
        fraud_scenario="none",
    )]


def generate_card_testing_attack():
    """Fraudster validates a stolen card with a rapid burst of small purchases on a fresh identity."""
    user_id = f"USR_{random.randint(10000, 99999)}"
    profile = USER_PROFILES.setdefault(user_id, {
        "home_country": random.choice(COUNTRIES),
        "known_devices": set(),
        "account_age_days": random.randint(0, 3),
        "last_txn_time": None,
        "recent_txn_times": [],
    })
    device = random.choice(DEVICES)
    ip_country = random.choice(COUNTRIES)

    events = []
    for _ in range(random.randint(5, 15)):
        amount = round(random.uniform(0.50, 3.00), 2)
        events.append(_base_event(
            user_id, profile, amount, random.choice(['digital_goods', 'gaming']), device,
            is_new_device=True,
            payment_method='card',
            ip_country=ip_country,
            is_fraud=True,
            fraud_scenario="card_testing",
        ))
    return events


def generate_account_takeover_attack():
    """Fraudster hijacks an existing account and cashes out with one large, out-of-pattern purchase."""
    if not USER_PROFILES:
        return generate_normal_event()
    user_id = random.choice(list(USER_PROFILES.keys()))
    profile = USER_PROFILES[user_id]
    foreign_countries = [c for c in COUNTRIES if c != profile["home_country"]]

    event = _base_event(
        user_id, profile,
        amount=round(random.uniform(800, 4999), 2),
        merchant_category=random.choice(HIGH_RISK_CATEGORIES),
        device=random.choice(DEVICES),
        is_new_device=True,
        payment_method=random.choice(PAYMENT_METHODS),
        ip_country=random.choice(foreign_countries),
        is_fraud=True,
        fraud_scenario="account_takeover",
    )
    return [event]


def generate_velocity_burst_attack():
    """Compromised session drives many rapid-fire, moderate-to-high value purchases from one account."""
    if not USER_PROFILES:
        return generate_normal_event()
    user_id = random.choice(list(USER_PROFILES.keys()))
    profile = USER_PROFILES[user_id]
    device = random.choice(list(profile["known_devices"]) or DEVICES)

    events = []
    for _ in range(random.randint(4, 10)):
        events.append(_base_event(
            user_id, profile,
            amount=round(random.uniform(150, 900), 2),
            merchant_category=random.choice(HIGH_RISK_CATEGORIES),
            device=device,
            is_new_device=False,
            payment_method=random.choice(PAYMENT_METHODS),
            ip_country=profile["home_country"],
            is_fraud=True,
            fraud_scenario="velocity_burst",
        ))
    return events


ATTACK_GENERATORS = [generate_card_testing_attack, generate_account_takeover_attack, generate_velocity_burst_attack]
ATTACK_PROBABILITY = 0.03  # fraction of ticks that spawn a fraud scenario burst instead of one normal event


def generate_events():
    if random.random() < ATTACK_PROBABILITY:
        return random.choice(ATTACK_GENERATORS)()
    return generate_normal_event()


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


def main():
    print(f"🚀 Starting live transaction simulator. Streaming to Kafka topic: '{TOPIC_NAME}'...")
    producer = create_kafka_producer()

    try:
        while True:
            for event in generate_events():
                producer.send(TOPIC_NAME, value=event)
                flag = "🚨 FRAUD" if event["is_fraud"] else "✅"
                print(f"{flag} {event['event_id']} | {event['fraud_scenario']} | user={event['user_id']} amount={event['amount']}")
                time.sleep(random.uniform(0.02, 0.1))

            time.sleep(random.uniform(0.2, 1.5))

    except KeyboardInterrupt:
        print("\n🛑 Simulator stopped by user. Flushing remaining messages...")
    finally:
        producer.flush()
        producer.close()
        print("🔌 Kafka connection safely closed.")


if __name__ == "__main__":
    main()
