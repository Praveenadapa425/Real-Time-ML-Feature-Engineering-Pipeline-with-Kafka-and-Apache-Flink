package com.pipeline.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UserEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("content_id")
    private String contentId;

    @JsonProperty("event_type")
    private String eventType;

    @JsonProperty("dwell_time_ms")
    private int dwellTimeMs;

    @JsonProperty("timestamp")
    private String timestamp;

    public UserEvent() {}

    public UserEvent(String userId, String contentId, String eventType, int dwellTimeMs, String timestamp) {
        this.userId = userId;
        this.contentId = contentId;
        this.eventType = eventType;
        this.dwellTimeMs = dwellTimeMs;
        this.timestamp = timestamp;
    }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getContentId() { return contentId; }
    public void setContentId(String contentId) { this.contentId = contentId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public int getDwellTimeMs() { return dwellTimeMs; }
    public void setDwellTimeMs(int dwellTimeMs) { this.dwellTimeMs = dwellTimeMs; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public long getTimestampEpoch() {
        try {
            return Instant.parse(timestamp).toEpochMilli();
        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }

    @Override
    public String toString() {
        return "UserEvent{" +
                "userId='" + userId + '\'' +
                ", contentId='" + contentId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", dwellTimeMs=" + dwellTimeMs +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
