FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (e.g. curl for health check)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy producer script (to be implemented in Phase 2)
COPY . .

# Environment defaults
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV KAFKA_TOPIC_USER_EVENTS=user-events
ENV KAFKA_TOPIC_CONTENT_METADATA=content-metadata

# Set user for security compliance
USER 1000

CMD ["python", "-u", "producer.py"]
