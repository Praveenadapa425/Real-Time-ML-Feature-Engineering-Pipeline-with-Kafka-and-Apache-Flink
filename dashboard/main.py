import asyncio
import threading
import json
import time
import logging
import sys
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("DashboardBackend")

app = FastAPI(title="Real-Time Feature Pipeline Observability Dashboard")

# Global State
features_cache = {}  # { entity_id: { feature_name: { value, computed_at, received_at } } }
metrics_state = {
    "late_events_dropped": 0,
    "current_watermark": 0,  # epoch ms
    "last_sim_time": 0,      # epoch ms
}

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"New client connected. Active clients: {len(self.active_connections)}")
        # Send initial state dump to the newly connected client
        initial_data = {
            "type": "state_dump",
            "features": features_cache,
            "metrics": metrics_state
        }
        await websocket.send_text(json.dumps(initial_data))

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"Client disconnected. Active clients: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_text(json.dumps(message))
            except Exception as e:
                # Handle dead connections
                pass

manager = ConnectionManager()

# Kafka consumer loop run in background threads
def kafka_consumer_thread():
    bootstrap_servers = ["kafka:29092"]
    logger.info(f"Starting background Kafka Consumer threads for bootstrap servers: {bootstrap_servers}")
    
    # Initialize consumer with retry loop
    consumer = None
    retries = 0
    max_retries = 10
    while retries < max_retries:
        try:
            consumer = KafkaConsumer(
                "feature-store",
                "flink-metrics",
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id="dashboard-visualizer-group",
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None
            )
            logger.info("Kafka Consumer initialized successfully.")
            break
        except KafkaError as e:
            retries += 1
            logger.warning(f"Kafka consumer connection attempt {retries}/{max_retries} failed: {e}. Retrying...")
            time.sleep(3)
            
    if not consumer:
        logger.error("Could not initialize Kafka consumer. Thread exiting.")
        return

    # Create event loop for async broadcasting inside synchronous thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        for msg in consumer:
            topic = msg.topic
            val = msg.value
            
            if not val:
                continue
                
            if topic == "feature-store":
                entity_id = val.get("entity_id")
                feature_name = val.get("feature_name")
                feature_value = val.get("feature_value")
                computed_at = val.get("computed_at")
                
                if entity_id and feature_name:
                    if entity_id not in features_cache:
                        features_cache[entity_id] = {}
                    
                    features_cache[entity_id][feature_name] = {
                        "value": feature_value,
                        "computed_at": computed_at,
                        "received_at": time.time()
                    }
                    
                    # Track last simulated time for watermark lag calculation
                    # The computed_at string is standard ISO 8601
                    try:
                        # Strip Z for parsing if needed
                        dt_str = computed_at.replace("Z", "")
                        dt = datetime.fromisoformat(dt_str)
                        metrics_state["last_sim_time"] = int(dt.timestamp() * 1000)
                    except Exception as e:
                        pass
                    
                    # Broadcast feature update
                    update_msg = {
                        "type": "feature_update",
                        "entity_id": entity_id,
                        "feature_name": feature_name,
                        "data": features_cache[entity_id][feature_name]
                    }
                    loop.run_until_complete(manager.broadcast(update_msg))
                    
            elif topic == "flink-metrics":
                metric_name = val.get("metric_name")
                metric_value = val.get("metric_value")
                
                if metric_name == "current_watermark":
                    metrics_state["current_watermark"] = int(metric_value)
                elif metric_name == "late_events_dropped":
                    # Since Flink outputs a metric value of 1.0 per late event, we increment our counter
                    metrics_state["late_events_dropped"] += int(metric_value)
                
                # Broadcast metrics update
                update_msg = {
                    "type": "metrics_update",
                    "metrics": metrics_state
                }
                loop.run_until_complete(manager.broadcast(update_msg))
                
    except Exception as e:
        logger.error(f"Error in Kafka consumer loop: {e}")
    finally:
        consumer.close()

# Start background thread on startup
@app.on_event("startup")
def start_kafka_listener():
    thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    thread.start()

# WebSocket endpoint
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection open and check for client messages (heartbeats, etc.)
            data = await websocket.receive_text()
            # Respond to client messages if needed
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# Serve Frontend HTML
@app.get("/")
async def get_index():
    # Read templates/index.html file
    try:
        with open("templates/index.html", "r") as f:
            html_content = f.read()
        return HTMLResponse(content=html_content, status_code=200)
    except FileNotFoundError:
        return HTMLResponse(content="<h1>Index page template not found!</h1>", status_code=404)

if __name__ == "__main__":
    import uvicorn
    import os
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)

