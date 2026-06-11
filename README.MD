# Real-Time ML Feature Engineering Pipeline with Kafka and Apache Flink

## Overview

This project implements a production-style real-time feature engineering pipeline using Apache Kafka and Apache Flink.

The system simulates user activity, processes event streams using event-time semantics, computes machine learning features in real time, and exposes the results through an observability dashboard.

The architecture demonstrates key stream processing concepts including:

* Kafka-based event ingestion
* Event-time processing
* Watermarks
* Tumbling and sliding windows
* Stream-table joins
* Stateful aggregations
* Real-time feature serving
* Dashboard observability

---

# Architecture

```text
Producer
    │
    ▼
Kafka Topics
 ├── user-events
 ├── content-metadata
 ├── feature-store
 └── flink-metrics
    │
    ▼
Apache Flink
 ├── User Features
 ├── Content Features
 ├── Category Affinity Features
 └── Metrics Generation
    │
    ▼
Dashboard Backend
    │
    ▼
Web Dashboard
```

---

# Components

## 1. Producer Service

The producer simulates user interactions and publishes events to Kafka.

Features:

* Multiple user archetypes
* Accelerated event generation
* Continuous streaming
* Content metadata generation
* Late event injection (approximately 5%)
* ISO-8601 event timestamps

Generated Topics:

* user-events
* content-metadata

---

## 2. Kafka

Kafka serves as:

### Event Bus

Stores:

* User interactions
* Content metadata
* Feature updates

### Feature Store

The feature-store topic is configured as a compacted topic and stores the latest feature value for each entity.

---

## 3. Apache Flink Pipeline

The Flink job consumes Kafka streams and computes real-time features.

### Event-Time Processing

Uses:

```java
WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(30))
```

### User Features

1-hour tumbling windows:

* click_rate
* avg_dwell_time

### Content Features

15-minute sliding window
5-minute slide interval

Feature:

* engagement_rate

### Stream-Table Join

User events are joined with content metadata to compute:

* category_affinity_score

### Metrics

The pipeline publishes operational metrics to:

```text
flink-metrics
```

Examples:

* current_watermark
* late_events_dropped

---

## 4. Dashboard

The dashboard provides real-time observability.

Features:

### Entity Viewer

Lookup by:

* user_id
* content_id

Displays:

* latest feature values
* feature timestamps
* ingestion age

### Real-Time Updates

Uses WebSockets to push updates to connected clients.

### Operational Metrics

Displays:

* Feature freshness
* Current watermark lag
* Late event metrics
* Real-time ingestion logs

---

# Kafka Topics

## user-events

Partitions: 3

Schema:

```json
{
  "user_id": "string",
  "content_id": "string",
  "event_type": "string",
  "dwell_time_ms": 1000,
  "timestamp": "2026-06-13T10:00:00Z"
}
```

---

## content-metadata

Compacted Topic

Schema:

```json
{
  "content_id": "string",
  "category": "string",
  "creator_id": "string",
  "publish_timestamp": "2026-06-13T10:00:00Z"
}
```

---

## feature-store

Compacted Topic

Schema:

```json
{
  "entity_id": "usr_007",
  "feature_name": "click_rate",
  "feature_value": 0.65,
  "computed_at": "2026-06-13T15:00:00Z"
}
```

---

## flink-metrics

Schema:

```json
{
  "metric_name": "current_watermark",
  "metric_value": 1781356915999,
  "computed_at": "2026-06-13T15:00:00Z"
}
```

---

# Prerequisites

Install:

* Docker
* Docker Compose

Recommended:

* Docker Desktop 4.x+
* 8 GB RAM minimum

---

# Environment Variables

Create a `.env` file from `.env.example`.

Example:

```env
KAFKA_BOOTSTRAP_SERVERS=kafka:29092

KAFKA_TOPIC_USER_EVENTS=user-events
KAFKA_TOPIC_CONTENT_METADATA=content-metadata
KAFKA_TOPIC_FEATURE_STORE=feature-store
KAFKA_TOPIC_METRICS=flink-metrics

DASHBOARD_PORT=8000
```

---

# Running the Project

## Build Services

```bash
docker compose build
```

## Start Services

```bash
docker compose up -d
```

## Verify Containers

```bash
docker compose ps
```

Expected services:

```text
zookeeper
kafka
flink-jobmanager
flink-taskmanager
producer
dashboard
flink-job-submitter
```

---

# Verify Kafka Topics

```bash
docker exec kafka kafka-topics \
--bootstrap-server localhost:9092 \
--list
```

Expected:

```text
user-events
content-metadata
feature-store
flink-metrics
```

---

# Access the Applications

## Flink Dashboard

```text
http://localhost:8081
```

---

## Observability Dashboard

```text
http://localhost:8000
```

---

# Verification Examples

## Consume Feature Records

```bash
docker exec kafka kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic feature-store \
--max-messages 10
```

---

## Consume Metrics

```bash
docker exec kafka kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic flink-metrics \
--max-messages 10
```

---

# Project Structure

```text
.
├── docker-compose.yml
├── .env.example
├── README.md
├── ANALYSIS.md
├── submission.json
│
├── producer/
│   ├── Dockerfile
│   └── producer.py
│
├── flink-job/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/
│
└── dashboard/
    ├── Dockerfile
    ├── main.py
    └── static/
```

---

# Test Entities

Example IDs generated by the producer:

User:

```text
usr_007
```

Content:

```text
cnt_010
```

These values are also provided in:

```text
submission.json
```

---

# Technologies Used

* Apache Kafka
* Apache Flink 1.18.1
* Java 11
* Maven
* Python
* FastAPI
* WebSockets
* Docker
* Docker Compose

---
