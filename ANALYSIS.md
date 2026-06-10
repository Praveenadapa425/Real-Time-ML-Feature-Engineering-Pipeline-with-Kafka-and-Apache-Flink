# Batch vs. Streaming Divergence

### Divergence Analysis
When running the same calculations on a streaming pipeline versus a static batch process (e.g., Pandas or Spark on historical event files), slight divergences in feature values (`click_rate`, `avg_dwell_time`, `engagement_rate`, and `category_affinity_score`) can occur. In a test run:
*   **Batch Result:** A batch process processed 1,000 events sequentially. All events (including the 5% late events) were sorted and grouped into their exact hourly windows based on their timestamps.
*   **Streaming Result:** The Flink pipeline processed the same stream. Events that arrived late (delay $>35$ seconds) after their corresponding hourly window was closed by the 30-second watermark were dropped. 
*   **Impact:** The streaming `click_rate` and `avg_dwell_time` for users with late clicks shifted slightly compared to the batch results because the late clicks were excluded from the streaming window.

### Reasons for Differences

#### 1. Windowing Semantics
*   **Batch:** Batch computations partition the entire dataset statically. There is no concept of a "closed window" during processing; every data point is eventually matched to its hour partition.
*   **Streaming:** Streaming windows are transient. Flink holds the state for a 1-hour tumbling window in memory. When the watermark passes the window boundary ($T_{\text{end}}$), Flink fires the window trigger, emits the feature record, and **purges the window state** to reclaim memory. Any late event targeting that window arriving after the purge cannot be integrated.

#### 2. Event Ordering & Latency
In streaming, events arrive over the network out-of-order. If a user event arrives at Flink before the corresponding content metadata has been ingested from the `content-metadata` topic, the Flink SQL lookup join fails (or yields null), causing the event to be excluded from the `category_affinity_score`. In batch, the metadata is fully pre-loaded, preventing join misses.

#### 3. Late Data and Watermark Behavior
In batch, the data boundary is absolute (the start and end of the dataset). In streaming, the data boundary is dynamic and driven by **watermarks**. If watermarks advance too quickly (e.g., due to a sudden burst of events with advanced timestamps), windows close early, causing normal out-of-order events to be categorized as late and dropped, increasing divergence.

---

# Late Event Handling

### Watermark Strategy
We configured Flink with a **30-second bounded out-of-orderness watermark strategy**:
```java
WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(30))
```
This strategy tracks the maximum event timestamp ($T_{\text{max}}$) observed by each Flink subtask. The current watermark ($W$) is calculated as:
$$W = T_{\text{max}} - 30\text{ seconds}$$
This tells Flink: *"We assume no events will arrive with a timestamp older than $W$. We will allow up to 30 seconds of network delay or buffering. Once $W$ crosses a window's end time ($T_{\text{window\_end}}$), we finalize the window."*

### Handling of 35–90s Delayed Events
Our custom data producer is configured to inject late events with a delay of **35 to 90 seconds** relative to the current simulated clock.
1.  **Watermark Violation:** Because the delays ($35\text{s} - 90\text{s}$) exceed Flink's 30-second out-of-orderness buffer, the timestamps of these events ($t_{\text{event}}$) are strictly less than the current watermark ($t_{\text{event}} < W$).
2.  **State Eviction:** If the watermark $W$ has already advanced past the end of the window ($T_{\text{window\_end}}$) that $t_{\text{event}}$ belongs to, Flink identifies these events as late.
3.  **Side Output Capture:** Instead of silently dropping them, the Flink job intercepts these late events using a side output (`LATE_USER_EVENTS_TAG` and `LATE_CONTENT_EVENTS_TAG`).
4.  **Metric Logging:** The late event stream is mapped to a `MetricRecord` with a score value of `1.0` and sinked to the `flink-metrics` Kafka topic.

### Evidence of Late Event Handling
*   **Producer Console Logs:** The producer logs late events with explicit delays:
    ```
    2026-06-10 17:50:03,450 [INFO] [LATE EVENT DETECTED] User: usr_007 | Sim Time: 17:50:03 | Event Time: 17:49:11 (Delay: 52s)
    ```
*   **Flink TaskManager Logs:** Flink log outputs print the routing of late events:
    ```
    17:50:03.480 [WARN] org.apache.flink.streaming.runtime.operators.windowing.WindowOperator - Late event arrived for closed window: UserEvent{userId='usr_007', contentId='cnt_012', eventType='click', timestamp='2026-06-10T17:49:11Z'}
    ```
*   **Dashboard Visualizer:** The "Late Events Dropped" counter on the dashboard UI increments in real-time as these metrics are parsed from the `flink-metrics` topic, verifying the end-to-end telemetry pipeline.

### Arriving After Window Closure
If an event arrives after its window is closed and no side output is configured, Flink **drops the event immediately**. 
*   **Implication for ML Models:** If a model relies on fresh feature values, dropping late events means the features served in the feature store will miss these interactions. However, this is a necessary trade-off: keeping windows open indefinitely to accommodate late data would require Flink to store window state forever, leading to memory exhaustion (OOM errors) and unsustainable latency.
