package zz.ks.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SinkModel {
    public final static String PARTITION_BY_KEY = "__key";
    String topic;
    String partitionBy;
    TimestampModel timestamp;

    @JsonCreator
    public SinkModel(@JsonProperty("topic") String topic,
                     @JsonProperty("partitionBy") String partitionBy,
                     @JsonProperty("timestamp") TimestampModel timestamp) {
        this.topic = topic;
        this.partitionBy = partitionBy;
        this.timestamp = timestamp;
    }

    @JsonProperty
    public String getTopic() {
        return topic;
    }

    @JsonProperty
    public String getPartitionBy() {
        return partitionBy;
    }

    @JsonProperty
    public TimestampModel getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append("topic: ").append(topic).append(", ")
                .append("partitionBy: ").append(partitionBy).append(", ")
                .append("timestamp: ").append(timestamp)
                .append("}");

        return builder.toString();    }
}
