import json
import time
import random
import os
from datetime import datetime, timedelta
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
USER_EVENTS_TOPIC = os.getenv('USER_EVENTS_TOPIC', 'user-events')
CONTENT_METADATA_TOPIC = os.getenv('CONTENT_METADATA_TOPIC', 'content-metadata')
ACCELERATION_FACTOR = int(os.getenv('SIMULATION_ACCELERATION_FACTOR', 60))
LATE_EVENT_PERIOD = float(os.getenv('LATE_EVENT_PERCENTAGE', 0.05))

# Setup Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: v.encode('utf-8') if v else None
)

USERS = [f"user_{i}" for i in range(1, 21)]
CONTENTS = [f"content_{i}" for i in range(1, 51)]
CATEGORIES = ["sci-fi", "news", "comedy", "documentary", "horror"]
EVENT_TYPES = ["view", "click", "like", "share"]

def generate_metadata():
    print("Generating content metadata...")
    for content_id in CONTENTS:
        metadata = {
            "content_id": content_id,
            "category": random.choice(CATEGORIES),
            "creator_id": f"creator_{random.randint(1, 10)}",
            "publish_timestamp": (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat()
        }
        producer.send(CONTENT_METADATA_TOPIC, key=content_id, value=metadata)
    producer.flush()
    print("Metadata populated.")

def run_simulation():
    print(f"Starting simulation with acceleration factor {ACCELERATION_FACTOR}x...")
    
    # Track simulation time
    sim_start_time = datetime.now()
    real_start_time = time.time()
    
    while True:
        # Calculate current simulation time
        elapsed_real_sec = time.time() - real_start_time
        sim_now = sim_start_time + timedelta(seconds=elapsed_real_sec * ACCELERATION_FACTOR)
        
        # Batch produce events for current block
        # We produce ~10 events per real second
        for _ in range(random.randint(5, 15)):
            user_id = random.choice(USERS)
            content_id = random.choice(CONTENTS)
            event_type = random.choice(EVENT_TYPES)
            
            # Determine processing time vs event time
            event_ts = sim_now
            
            # 5% chance of being late
            is_late = random.random() < LATE_EVENT_PERIOD
            if is_late:
                # 35 to 90 seconds in the past relative to simulation clock
                lag_sec = random.uniform(35, 90)
                event_ts = sim_now - timedelta(seconds=lag_sec)
            
            event = {
                "user_id": user_id,
                "content_id": content_id,
                "event_type": event_type,
                "dwell_time_ms": random.randint(1000, 300000) if event_type == "view" else 0,
                "timestamp": event_ts.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + "Z"
            }
            
            producer.send(USER_EVENTS_TOPIC, key=user_id, value=event)
            
            if is_late:
                print(f"[LATE] Produced late event for {user_id} with lag {event_ts.isoformat()} (Current sim: {sim_now.isoformat()})")

        producer.flush()
        time.sleep(1) # Wait 1 real second

if __name__ == "__main__":
    generate_metadata()
    try:
        run_simulation()
    except KeyboardInterrupt:
        print("Producer stopped.")
