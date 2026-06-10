package com.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.models.UserEvent;
import com.pipeline.models.FeatureRecord;
import com.pipeline.models.MetricRecord;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.delivery.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class FeaturePipeline {
    private static final Logger LOG = LoggerFactory.getLogger(FeaturePipeline.class);

    // Side output tags for capturing late events
    private static final OutputTag<UserEvent> LATE_USER_EVENTS_TAG = new OutputTag<UserEvent>("late-user-events") {};
    private static final OutputTag<UserEvent> LATE_CONTENT_EVENTS_TAG = new OutputTag<UserEvent>("late-content-events") {};
    private static final OutputTag<MetricRecord> WATERMARK_METRICS_TAG = new OutputTag<MetricRecord>("watermark-metrics") {};

    public static void main(String[] args) throws Exception {
        LOG.info("Initializing Real-Time Feature Engineering Flink Pipeline...");

        // 1. Parse configs from Environment Variables
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrapServers == null) bootstrapServers = "kafka:29092";

        String userEventsTopic = System.getenv("KAFKA_TOPIC_USER_EVENTS");
        if (userEventsTopic == null) userEventsTopic = "user-events";

        String contentMetadataTopic = System.getenv("KAFKA_TOPIC_CONTENT_METADATA");
        if (contentMetadataTopic == null) contentMetadataTopic = "content-metadata";

        String featureStoreTopic = System.getenv("KAFKA_TOPIC_FEATURE_STORE");
        if (featureStoreTopic == null) featureStoreTopic = "feature-store";

        String metricsTopic = System.getenv("KAFKA_TOPIC_METRICS");
        if (metricsTopic == null) metricsTopic = "flink-metrics";

        LOG.info("Configuration: Kafka={}, user-events={}, content-metadata={}, feature-store={}, metrics={}",
                bootstrapServers, userEventsTopic, contentMetadataTopic, featureStoreTopic, metricsTopic);

        // 2. Set up Execution Environments (DataStream & Table API)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Enable Checkpointing for Fault Tolerance (Required for Stateful Stream Joins)
        env.enableCheckpointing(10000); // Checkpoint every 10 seconds

        // 3. Define Kafka Source for user-events
        KafkaSource<String> userEventsKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(userEventsTopic)
                .setGroupId("flink-feature-pipeline-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 4. Ingest and parse user-events DataStream
        DataStream<UserEvent> rawUserEvents = env.fromSource(
                userEventsKafkaSource,
                WatermarkStrategy.noWatermarks(),
                "UserEventsKafkaSource"
        ).map(new MapFunction<String, UserEvent>() {
            private transient ObjectMapper mapper;
            @Override
            public UserEvent map(String value) throws Exception {
                if (mapper == null) {
                    mapper = new ObjectMapper();
                }
                return mapper.readValue(value, UserEvent.class);
            }
        }).name("ParseUserEventsJson");

        // 5. Define Watermark Strategy with EXACTLY 30 seconds bounded out-of-orderness
        WatermarkStrategy<UserEvent> watermarkStrategy = WatermarkStrategy
                .<UserEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserEvent>() {
                    @Override
                    public long extractTimestamp(UserEvent event, long recordTimestamp) {
                        return event.getTimestampEpoch();
                    }
                });

        // Apply watermarks and assign event times
        DataStream<UserEvent> userEvents = rawUserEvents
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("AssignWatermarks30s");

        // 6. Watermark Observability: Periodically emit watermark to metrics
        SingleOutputStreamOperator<UserEvent> monitoredUserEvents = userEvents
                .process(new ProcessFunction<UserEvent, UserEvent>() {
                    private long lastEmitTime = 0;

                    @Override
                    public void processElement(UserEvent value, Context ctx, Collector<UserEvent> out) throws Exception {
                        long currentRealTime = System.currentTimeMillis();
                        // Emit watermark statistics to side output once every 5 seconds
                        if (currentRealTime - lastEmitTime > 5000) {
                            long watermark = ctx.timerService().currentWatermark();
                            if (watermark > Long.MIN_VALUE) {
                                ctx.output(
                                        WATERMARK_METRICS_TAG,
                                        new MetricRecord("current_watermark", (double) watermark, Instant.now().toString())
                                );
                                lastEmitTime = currentRealTime;
                            }
                        }
                        out.collect(value);
                    }
                })
                .name("WatermarkMetricsProcessor");

        // 7. Compute Per-User Features (1-hour Tumbling Window) with Late Event Side-Outputs
        SingleOutputStreamOperator<FeatureRecord> userFeatures = monitoredUserEvents
                .keyBy(UserEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .sideOutputLateData(LATE_USER_EVENTS_TAG)
                .aggregate(new UserFeatureAggregator(), new UserFeatureProcessWindow())
                .name("TumblingUserFeatures1H");

        // 8. Compute Per-Content Features (15-min Sliding Window, 5-min slide) with Late Event Side-Outputs
        SingleOutputStreamOperator<FeatureRecord> contentFeatures = monitoredUserEvents
                .keyBy(UserEvent::getContentId)
                .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.minutes(5)))
                .sideOutputLateData(LATE_CONTENT_EVENTS_TAG)
                .aggregate(new ContentFeatureAggregator(), new ContentFeatureProcessWindow())
                .name("SlidingContentFeatures15m5m");

        // 9. Stream-Table Join & Category Affinity using Flink SQL
        // Register userEvents stream as temporary view
        tEnv.createTemporaryView("user_events_view", monitoredUserEvents);

        // Register content-metadata compacted Kafka topic as SQL Table using upsert-kafka
        tEnv.executeSql(
                "CREATE TABLE content_metadata_table (\n" +
                "    content_id STRING,\n" +
                "    category STRING,\n" +
                "    creator_id STRING,\n" +
                "    publish_timestamp STRING,\n" +
                "    PRIMARY KEY (content_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = '" + contentMetadataTopic + "',\n" +
                "    'properties.bootstrap.servers' = '" + bootstrapServers + "',\n" +
                "    'key.format' = 'raw',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false',\n" +
                "    'value.json.ignore-parse-errors' = 'true'\n" +
                ")"
        );

        // Perform Stream-Table join and window aggregation in SQL
        Table affinityTable = tEnv.sqlQuery(
                "SELECT\n" +
                "  u.userId AS entity_id,\n" +
                "  CONCAT('category_affinity_score:', c.category) AS feature_name,\n" +
                "  CAST(COUNT(*) AS DOUBLE) AS feature_value,\n" +
                "  DATE_FORMAT(TUMBLE_END(TO_TIMESTAMP_LTZ(CAST(u.timestampEpoch AS BIGINT), 3), INTERVAL '1' HOUR), 'yyyy-MM-dd''T''HH:mm:ss''Z''') AS computed_at\n" +
                "FROM user_events_view u\n" +
                "JOIN content_metadata_table c ON u.contentId = c.content_id\n" +
                "GROUP BY\n" +
                "  u.userId,\n" +
                "  c.category,\n" +
                "  TUMBLE(TO_TIMESTAMP_LTZ(CAST(u.timestampEpoch AS BIGINT), 3), INTERVAL '1' HOUR)"
        );

        // Convert the SQL Table back to a DataStream of FeatureRecords
        DataStream<FeatureRecord> affinityFeatures = tEnv.toDataStream(affinityTable, FeatureRecord.class)
                .name("CategoryAffinityFeaturesStream");

        // 10. Merge all Feature Streams
        DataStream<FeatureRecord> allFeatures = userFeatures
                .union(contentFeatures)
                .union(affinityFeatures);

        // 11. Define Kafka Sink for features
        KafkaSink<FeatureRecord> featureKafkaSink = KafkaSink.<FeatureRecord>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new FeatureRecordSerializer(featureStoreTopic))
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        allFeatures.sinkTo(featureKafkaSink).name("FeatureStoreKafkaSink");

        // 12. Capture and Merge Late Event streams, mapping to MetricRecords
        DataStream<UserEvent> lateUserEvents = userFeatures.getSideOutput(LATE_USER_EVENTS_TAG);
        DataStream<UserEvent> lateContentEvents = contentFeatures.getSideOutput(LATE_CONTENT_EVENTS_TAG);
        DataStream<MetricRecord> watermarkMetrics = monitoredUserEvents.getSideOutput(WATERMARK_METRICS_TAG);

        DataStream<MetricRecord> lateMetrics = lateUserEvents
                .union(lateContentEvents)
                .map(new MapFunction<UserEvent, MetricRecord>() {
                    @Override
                    public MetricRecord map(UserEvent value) {
                        // Emit a drop count score of 1.0 per late event
                        return new MetricRecord("late_events_dropped", 1.0, Instant.now().toString());
                    }
                })
                .name("LateEventDropMetricsMap");

        // Combine all metrics and sink to flink-metrics topic
        DataStream<MetricRecord> allMetrics = watermarkMetrics.union(lateMetrics);

        KafkaSink<MetricRecord> metricsKafkaSink = KafkaSink.<MetricRecord>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new MetricRecordSerializer(metricsTopic))
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        allMetrics.sinkTo(metricsKafkaSink).name("FlinkMetricsKafkaSink");

        // 13. Execute the Flink Streaming Job
        env.execute("Real-Time-ML-Feature-Engineering-Pipeline");
    }

    // ==========================================================
    // Aggregation Logic Classes (Java Serializable)
    // ==========================================================

    public static class UserAccumulator implements Serializable {
        private static final long serialVersionUID = 1L;
        public int totalEvents = 0;
        public int clickEvents = 0;
        public long totalDwellTime = 0;
    }

    public static class UserFeatureAggregator implements AggregateFunction<UserEvent, UserAccumulator, UserAccumulator> {
        private static final long serialVersionUID = 1L;

        @Override
        public UserAccumulator createAccumulator() { return new UserAccumulator(); }

        @Override
        public UserAccumulator add(UserEvent value, UserAccumulator accum) {
            accum.totalEvents++;
            if ("click".equalsIgnoreCase(value.getEventType())) {
                accum.clickEvents++;
            }
            accum.totalDwellTime += value.getDwellTimeMs();
            return accum;
        }

        @Override
        public UserAccumulator getResult(UserAccumulator accum) { return accum; }

        @Override
        public UserAccumulator merge(UserAccumulator a, UserAccumulator b) {
            a.totalEvents += b.totalEvents;
            a.clickEvents += b.clickEvents;
            a.totalDwellTime += b.totalDwellTime;
            return a;
        }
    }

    public static class UserFeatureProcessWindow extends ProcessWindowFunction<UserAccumulator, FeatureRecord, String, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void process(String userId, Context context, Iterable<UserAccumulator> elements, Collector<FeatureRecord> out) {
            UserAccumulator acc = elements.iterator().next();
            String computedAt = Instant.ofEpochMilli(context.window().getEnd()).toString();

            double clickRate = acc.totalEvents > 0 ? (double) acc.clickEvents / acc.totalEvents : 0.0;
            double avgDwellTime = acc.totalEvents > 0 ? (double) acc.totalDwellTime / acc.totalEvents : 0.0;

            out.collect(new FeatureRecord(userId, "click_rate", clickRate, computedAt));
            out.collect(new FeatureRecord(userId, "avg_dwell_time", avgDwellTime, computedAt));
        }
    }

    public static class ContentAccumulator implements Serializable {
        private static final long serialVersionUID = 1L;
        public int views = 0;
        public int engagementCount = 0;
    }

    public static class ContentFeatureAggregator implements AggregateFunction<UserEvent, ContentAccumulator, ContentAccumulator> {
        private static final long serialVersionUID = 1L;

        @Override
        public ContentAccumulator createAccumulator() { return new ContentAccumulator(); }

        @Override
        public ContentAccumulator add(UserEvent value, ContentAccumulator accum) {
            String type = value.getEventType();
            if ("view".equalsIgnoreCase(type)) {
                accum.views++;
            } else if ("like".equalsIgnoreCase(type) || "share".equalsIgnoreCase(type)) {
                accum.engagementCount++;
            }
            return accum;
        }

        @Override
        public ContentAccumulator getResult(ContentAccumulator accum) { return accum; }

        @Override
        public ContentAccumulator merge(ContentAccumulator a, ContentAccumulator b) {
            a.views += b.views;
            a.engagementCount += b.engagementCount;
            return a;
        }
    }

    public static class ContentFeatureProcessWindow extends ProcessWindowFunction<ContentAccumulator, FeatureRecord, String, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void process(String contentId, Context context, Iterable<ContentAccumulator> elements, Collector<FeatureRecord> out) {
            ContentAccumulator acc = elements.iterator().next();
            String computedAt = Instant.ofEpochMilli(context.window().getEnd()).toString();

            // Prevent division-by-zero
            double engagementRate = acc.views > 0 ? (double) acc.engagementCount / acc.views : 0.0;

            out.collect(new FeatureRecord(contentId, "engagement_rate", engagementRate, computedAt));
        }
    }

    // ==========================================================
    // Serialization Schema Classes for Kafka Sinks
    // ==========================================================

    public static class FeatureRecordSerializer implements KafkaRecordSerializationSchema<FeatureRecord> {
        private static final long serialVersionUID = 1L;
        private final String topic;
        private transient ObjectMapper mapper;

        public FeatureRecordSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(FeatureRecord element, KafkaSinkContext context, Long timestamp) {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            try {
                String key = element.getEntityId() + ":" + element.getFeatureName();
                byte[] keyBytes = key.getBytes("UTF-8");
                byte[] valueBytes = mapper.writeValueAsBytes(element);
                return new ProducerRecord<>(topic, keyBytes, valueBytes);
            } catch (Exception e) {
                LOG.error("Failed to serialize FeatureRecord: " + element, e);
                return null;
            }
        }
    }

    public static class MetricRecordSerializer implements KafkaRecordSerializationSchema<MetricRecord> {
        private static final long serialVersionUID = 1L;
        private final String topic;
        private transient ObjectMapper mapper;

        public MetricRecordSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(MetricRecord element, KafkaSinkContext context, Long timestamp) {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            try {
                byte[] keyBytes = element.getMetricName().getBytes("UTF-8");
                byte[] valueBytes = mapper.writeValueAsBytes(element);
                return new ProducerRecord<>(topic, keyBytes, valueBytes);
            } catch (Exception e) {
                LOG.error("Failed to serialize MetricRecord: " + element, e);
                return null;
            }
        }
    }
}
