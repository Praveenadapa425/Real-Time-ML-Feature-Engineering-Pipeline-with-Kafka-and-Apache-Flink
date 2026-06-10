FROM python:3.11-slim

WORKDIR /app

# Install curl for healthchecks
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy dashboard codebase (to be implemented in Phase 4)
COPY . .

EXPOSE 8000

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV KAFKA_TOPIC_FEATURE_STORE=feature-store

# Set user for security compliance
USER 1000

CMD ["python", "-u", "main.py"]
