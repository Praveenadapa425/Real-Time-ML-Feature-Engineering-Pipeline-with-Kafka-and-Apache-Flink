import os

class Config:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    KAFKA_TOPIC_USER_EVENTS = os.getenv("KAFKA_TOPIC_USER_EVENTS", "user-events")
    KAFKA_TOPIC_CONTENT_METADATA = os.getenv("KAFKA_TOPIC_CONTENT_METADATA", "content-metadata")
    
    # 5% default late event rate
    LATE_EVENT_PERCENTAGE = float(os.getenv("LATE_EVENT_PERCENTAGE", "0.05"))
    
    # Default: 1 real minute = 1 simulated hour -> 1 real second = 60 simulated seconds
    # Calculation: 3600 simulated seconds / 60 real seconds = 60.0
    ACCELERATION_FACTOR = float(os.getenv("ACCELERATION_FACTOR", "60.0"))

    # Simulator execution configuration
    METADATA_COUNT = int(os.getenv("METADATA_COUNT", "50"))
    MAX_USERS = int(os.getenv("MAX_USERS", "20"))
    SIMULATION_TICK_SPEED_SEC = float(os.getenv("SIMULATION_TICK_SPEED_SEC", "1.0")) # ticks per real second
