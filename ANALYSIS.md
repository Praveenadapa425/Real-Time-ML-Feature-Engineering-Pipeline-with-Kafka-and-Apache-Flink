# Batch vs. Streaming Divergence

## Divergence Analysis

The same feature calculations can produce slightly different results when executed in a streaming system versus a batch processing system.

For comparison, the streaming pipeline continuously processed events from Kafka using Apache Flink, while a hypothetical batch process would read the complete historical dataset after all events were generated and compute the same feature values.

During testing, the producer generated both normal events and deliberately delayed events. The streaming pipeline processed events according to event-time semantics and watermark progression, while a batch process would have access to all events before computing aggregates.

As a result, small differences may occur in feature values such as:

* `click_rate`
* `avg_dwell_time`
* `engagement_rate`
* `category_affinity_score`

These differences are expected and are a natural consequence of streaming window execution and late-event handling.

## Reasons for Differences

### 1. Windowing Semantics

In batch processing, the entire dataset is available before computation begins. Events can be sorted and assigned to their correct windows without any concern for arrival order.

In the streaming pipeline, Flink maintains state for active windows and emits results when event-time windows are finalized. Once a window is closed, the state associated with that window is released. Events arriving after window closure may not contribute to previously emitted results.

### 2. Event Ordering

Streaming systems process events as they arrive. Network delays, producer buffering, and partition ordering can cause events to arrive out of order.

Batch systems process a complete dataset where ordering can be reconstructed before aggregation. Because of this, batch results represent a fully materialized view of the data, whereas streaming results reflect the state of the system at the time windows are evaluated.

### 3. Watermark Progression

The streaming pipeline uses event-time processing with watermarks to determine when a window can be safely finalized.

As watermarks advance, Flink assumes that events older than the watermark are unlikely to arrive. This enables low-latency feature generation but can introduce small differences compared to a batch process that waits for all data before computing results.

## Impact on Machine Learning Features

These divergences are generally small and represent a common trade-off in real-time systems.

The benefit of streaming features is freshness. A recommendation model receiving features updated every few seconds or minutes can react to user behavior much faster than a batch system that updates features only periodically.

For recommendation and personalization workloads, fresh features typically provide greater value than perfectly complete historical aggregates.

# Late Event Handling

## Watermark Strategy

The Flink pipeline is configured with a bounded out-of-orderness watermark strategy of exactly 30 seconds:

```java
WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(30))
```

The watermark is calculated from the highest event timestamp observed by the stream processor.

Conceptually:

Watermark = Maximum Event Timestamp Seen - 30 Seconds

This configuration allows Flink to tolerate events arriving up to 30 seconds late while still producing deterministic event-time results.

## Handling of Delayed Events

The producer intentionally generates approximately 5% of events with timestamps delayed between 35 and 90 seconds relative to the simulated event clock.

This behavior is used to test the pipeline's handling of out-of-order and delayed data.

When delayed events arrive:

1. Flink compares the event timestamp with the current watermark.
2. Events that arrive within the allowed watermark tolerance may still be incorporated into active windows.
3. Events that arrive after the relevant window has already been finalized are treated as late events.
4. The pipeline routes these events through dedicated late-event handling logic and publishes operational metrics to the `flink-metrics` topic.

## Evidence of Late Event Handling

### Producer Logs

The producer continuously generated delayed events during testing.

Example:

```text
[LATE EVENT DETECTED] User: usr_011 | Sim Time: 10:30:38 | Event Time: 10:29:35 (Delay: 63s)
```

Additional examples observed during execution included delays of 52 seconds, 69 seconds, and 75 seconds.

### Watermark Metrics

The `flink-metrics` Kafka topic continuously emitted watermark metrics such as:

```json
{
  "metric_name": "current_watermark",
  "metric_value": 1781356915999
}
```

These metrics confirmed that event-time processing and watermark advancement were functioning correctly throughout the execution of the Flink job.

### Dashboard Verification

The observability dashboard successfully displayed:

* Current watermark information
* Watermark lag calculations
* Feature freshness metrics
* Real-time feature updates from the `feature-store` topic

This verified the end-to-end telemetry flow from Flink to Kafka and ultimately to the dashboard.

## Arriving After Window Closure

If an event arrives after its corresponding window has already been finalized and the watermark has advanced beyond that window, the event can no longer modify the previously emitted aggregate.

This behavior is intentional.

Keeping windows open indefinitely would increase state size, memory usage, and processing latency. Watermarks provide a controlled trade-off between completeness and responsiveness.

For machine learning systems, this trade-off enables near real-time feature generation while maintaining predictable resource consumption and processing performance.
