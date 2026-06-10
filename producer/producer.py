import time
import json
import random
import logging
import sys
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Producer")

# Archetype Specifications
ARCHETYPES = {
    "binge_watcher": {
        "categories": ["Movies", "TV-Shows", "Documentaries"],
        "event_type_weights": {"view": 0.8, "click": 0.05, "like": 0.12, "share": 0.03},
        "dwell_time_range": (30000, 300000),      # 30s to 5 mins
        "activity_frequency_sec": (60, 300)       # Simulated gap between actions (1-5 min)
    },
    "news_scanner": {
        "categories": ["News", "Politics", "Tech", "Finance"],
        "event_type_weights": {"view": 0.3, "click": 0.6, "like": 0.07, "share": 0.03},
        "dwell_time_range": (1000, 15000),         # 1s to 15s
        "activity_frequency_sec": (10, 45)         # Simulated gap (10-45s)
    },
    "casual_browser": {
        "categories": ["Sports", "Entertainment", "Lifestyle", "Fashion"],
        "event_type_weights": {"view": 0.5, "click": 0.25, "like": 0.2, "share": 0.05},
        "dwell_time_range": (5000, 45000),         # 5s to 45s
        "activity_frequency_sec": (30, 120)        # Simulated gap (30s-2m)
    },
    "highly_engaged_user": {
        "categories": ["Tech", "Gaming", "Music", "Education"],
        "event_type_weights": {"view": 0.2, "click": 0.3, "like": 0.4, "share": 0.1},
        "dwell_time_range": (10000, 120000),       # 10s to 2 mins
        "activity_frequency_sec": (10, 60)         # Simulated gap (10-60s)
    }
}

# Pre-defined user pool mapped to archetypes
USERS = [
    {"user_id": f"usr_{i:03d}", "archetype": random.choice(list(ARCHETYPES.keys()))}
    for i in range(1, Config.MAX_USERS + 1)
]

def create_kafka_producer():
    """Initializes and returns a KafkaProducer instance with retries."""
    logger.info(f"Connecting to Kafka brokers at: {Config.KAFKA_BOOTSTRAP_SERVERS}")
    retries = 0
    max_retries = 10
    while retries < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5,
                acks=1
            )
            logger.info("Kafka Producer connected successfully.")
            return producer
        except KafkaError as e:
            retries += 1
            logger.warning(f"Failed to connect to Kafka (Attempt {retries}/{max_retries}): {e}. Retrying in 3 seconds...")
            time.sleep(3)
    logger.error("Could not connect to Kafka. Exiting.")
    sys.exit(1)

def generate_static_metadata(producer):
    """Generates and publishes a pool of content metadata items on startup."""
    logger.info("Generating content metadata records...")
    categories = ["Movies", "TV-Shows", "Documentaries", "News", "Politics", "Tech", "Finance", "Sports", "Entertainment", "Lifestyle", "Fashion", "Gaming", "Music", "Education"]
    content_pool = []
    
    for i in range(1, Config.METADATA_COUNT + 1):
        content_id = f"cnt_{i:03d}"
        category = random.choice(categories)
        creator_id = f"crt_{random.randint(1, 10):03d}"
        
        # Publish timestamp in standard ISO 8601
        publish_timestamp = (datetime.utcnow() - timedelta(days=random.randint(1, 30))).isoformat() + "Z"
        
        metadata = {
            "content_id": content_id,
            "category": category,
            "creator_id": creator_id,
            "publish_timestamp": publish_timestamp
        }
        
        content_pool.append(metadata)
        
        # Publish with content_id as partitioning key
        future = producer.send(
            Config.KAFKA_TOPIC_CONTENT_METADATA,
            key=content_id,
            value=metadata
        )
        # Synchronous send block to guarantee completion before starting stream
        future.get(timeout=10)
        
    logger.info(f"Published {len(content_pool)} metadata records to topic '{Config.KAFKA_TOPIC_CONTENT_METADATA}'.")
    return content_pool

def select_event_type(weights):
    """Selects an event type according to archetype weights."""
    types = list(weights.keys())
    probabilities = list(weights.values())
    return random.choices(types, weights=probabilities, k=1)[0]

def main():
    producer = create_kafka_producer()
    
    # 1. Publish metadata records first
    content_pool = generate_static_metadata(producer)
    
    # Wait to ensure Flink has processed metadata changelog before events start
    logger.info("Sleeping for 5 seconds to ensure metadata topics settle...")
    time.sleep(5)
    
    # 2. Initialize simulation variables
    logger.info("Starting simulation user stream...")
    real_start_time = datetime.utcnow()
    # Simulated clock starts at current UTC time
    sim_clock_start_time = datetime.utcnow()
    
    # Setup initial action times for users
    for user in USERS:
        archetype = ARCHETYPES[user["archetype"]]
        initial_offset_sec = random.randint(0, 30) # offset actions on startup
        user["next_sim_action_time"] = sim_clock_start_time + timedelta(seconds=initial_offset_sec)
        
    total_events = 0
    total_late_events = 0
    
    # Keep track of last metrics print to avoid spamming
    last_metrics_print = real_start_time

    while True:
        real_now = datetime.utcnow()
        real_elapsed = (real_now - real_start_time).total_seconds()
        
        # Accelerated simulation time calculation:
        # current_sim_time = sim_start + (real_elapsed * acceleration_factor)
        sim_elapsed_seconds = real_elapsed * Config.ACCELERATION_FACTOR
        current_sim_time = sim_clock_start_time + timedelta(seconds=sim_elapsed_seconds)
        
        for user in USERS:
            if current_sim_time >= user["next_sim_action_time"]:
                # User action is due!
                user_id = user["user_id"]
                arch_name = user["archetype"]
                arch = ARCHETYPES[arch_name]
                
                # Filter content matching archetype interest, or pick random
                matching_content = [c for c in content_pool if c["category"] in arch["categories"]]
                if not matching_content:
                    matching_content = content_pool
                content_item = random.choice(matching_content)
                content_id = content_item["content_id"]
                
                event_type = select_event_type(arch["event_type_weights"])
                dwell_time = random.randint(*arch["dwell_time_range"]) if event_type in ["view", "click"] else 0
                
                # Determine if this event should be produced as a 'late event'
                is_late = random.random() < Config.LATE_EVENT_PERCENTAGE
                
                if is_late:
                    # Late event: timestamp is between 35 and 90 seconds behind simulated current time
                    late_offset_sec = random.randint(35, 90)
                    event_sim_time = current_sim_time - timedelta(seconds=late_offset_sec)
                    total_late_events += 1
                    logger.info(
                        f"[LATE EVENT DETECTED] User: {user_id} ({arch_name}) | "
                        f"Sim Time: {current_sim_time.strftime('%H:%M:%S')} | "
                        f"Event Time: {event_sim_time.strftime('%H:%M:%S')} (Delay: {late_offset_sec}s)"
                    )
                else:
                    # Normal event: timestamp matches current simulated time
                    event_sim_time = current_sim_time
                    logger.info(
                        f"[NORMAL EVENT] User: {user_id} ({arch_name}) | "
                        f"Event Time: {event_sim_time.strftime('%H:%M:%S')}"
                    )
                
                # Construct JSON message payload
                message = {
                    "user_id": user_id,
                    "content_id": content_id,
                    "event_type": event_type,
                    "dwell_time_ms": dwell_time,
                    "timestamp": event_sim_time.strftime("%Y-%m-%dT%H:%M:%SZ")
                }
                
                # Publish to Kafka
                try:
                    producer.send(
                        Config.KAFKA_TOPIC_USER_EVENTS,
                        key=user_id,
                        value=message
                    )
                    total_events += 1
                except Exception as e:
                    logger.error(f"Failed to send message to Kafka: {e}")
                
                # Schedule next action simulated time
                gap_sec = random.randint(*arch["activity_frequency_sec"])
                user["next_sim_action_time"] = current_sim_time + timedelta(seconds=gap_sec)
                
        # Periodically log producer metrics (every 10 real seconds)
        if (real_now - last_metrics_print).total_seconds() >= 10.0:
            late_ratio = (total_late_events / total_events * 100) if total_events > 0 else 0
            logger.info(
                f"=== PRODUCER STATS | Total Produced: {total_events} | "
                f"Late Produced: {total_late_events} ({late_ratio:.2f}%) | "
                f"Configured Late %: {Config.LATE_EVENT_PERCENTAGE * 100}% | "
                f"Sim Clock: {current_sim_time.strftime('%Y-%m-%dT%H:%M:%SZ')} ==="
            )
            last_metrics_print = real_now
            
        # Tick delay (run simulation loop frequently to keep it accurate)
        # Since 1 real second = 60 simulated seconds, we sleep 0.1 real seconds (6 simulated seconds)
        time.sleep(1.0 / Config.SIMULATION_TICK_SPEED_SEC)

if __name__ == "__main__":
    main()
