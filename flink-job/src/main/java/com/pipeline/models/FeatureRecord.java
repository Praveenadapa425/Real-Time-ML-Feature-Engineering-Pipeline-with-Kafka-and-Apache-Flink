package com.pipeline.models;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FeatureRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("entity_id")
    private String entityId;

    @JsonProperty("feature_name")
    private String featureName;

    @JsonProperty("feature_value")
    private Double featureValue;

    @JsonProperty("computed_at")
    private String computedAt;

    public FeatureRecord() {}

    public FeatureRecord(String entityId, String featureName, Double featureValue, String computedAt) {
        this.entityId = entityId;
        this.featureName = featureName;
        this.featureValue = featureValue;
        this.computedAt = computedAt;
    }

    public String getEntityId() { return entityId; }
    public void setEntityId(String entityId) { this.entityId = entityId; }

    public String getFeatureName() { return featureName; }
    public void setFeatureName(String featureName) { this.featureName = featureName; }

    public Double getFeatureValue()     { return featureValue; }
    public void setFeatureValue(Double featureValue) { this.featureValue = featureValue; }

    public String getComputedAt() { return computedAt; }
    public void setComputedAt(String computedAt) { this.computedAt = computedAt; }

    @Override
    public String toString() {
        return "FeatureRecord{" +
                "entityId='" + entityId + '\'' +
                ", featureName='" + featureName + '\'' +
                ", featureValue=" + featureValue +
                ", computedAt='" + computedAt + '\'' +
                '}';
    }
}
