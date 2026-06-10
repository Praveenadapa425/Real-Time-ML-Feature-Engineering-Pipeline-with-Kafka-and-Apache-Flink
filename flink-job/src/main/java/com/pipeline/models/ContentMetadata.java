package com.pipeline.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ContentMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("content_id")
    private String contentId;

    @JsonProperty("category")
    private String category;

    @JsonProperty("creator_id")
    private String creatorId;

    @JsonProperty("publish_timestamp")
    private String publishTimestamp;

    public ContentMetadata() {}

    public ContentMetadata(String contentId, String category, String creatorId, String publishTimestamp) {
        this.contentId = contentId;
        this.category = category;
        this.creatorId = creatorId;
        this.publishTimestamp = publishTimestamp;
    }

    public String getContentId() { return contentId; }
    public void setContentId(String contentId) { this.contentId = contentId; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public String getCreatorId() { return creatorId; }
    public void setCreatorId(String creatorId) { this.creatorId = creatorId; }

    public String getPublishTimestamp() { return publishTimestamp; }
    public void setPublishTimestamp(String publishTimestamp) { this.publishTimestamp = publishTimestamp; }

    @Override
    public String toString() {
        return "ContentMetadata{" +
                "contentId='" + contentId + '\'' +
                ", category='" + category + '\'' +
                ", creatorId='" + creatorId + '\'' +
                ", publishTimestamp='" + publishTimestamp + '\'' +
                '}';
    }
}
