package com.pipeline.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class MetricRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("metric_name")
    private String metricName;

    @JsonProperty("metric_value")
    private double metricValue;

    @JsonProperty("computed_at")
    private String computedAt;

    public MetricRecord() {}

    public MetricRecord(String metricName, double metricValue, String computedAt) {
        this.metricName = metricName;
        this.metricValue = metricValue;
        this.computedAt = computedAt;
    }

    public String getMetricName() { return metricName; }
    public void setMetricName(String metricName) { this.metricName = metricName; }

    public double getMetricValue() { return metricValue; }
    public void setMetricValue(double metricValue) { this.metricValue = metricValue; }

    public String getComputedAt() { return computedAt; }
    public void setComputedAt(String computedAt) { this.computedAt = computedAt; }

    @Override
    public String toString() {
        return "MetricRecord{" +
                "metricName='" + metricName + '\'' +
                ", metricValue=" + metricValue +
                ", computedAt='" + computedAt + '\'' +
                '}';
    }
}
