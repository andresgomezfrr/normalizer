package rb.ks.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SinkModel {
    public final static String PARTITION_BY_KEY = "__KEY";
    String topic;
    String partitionBy;

    @JsonCreator
    public SinkModel(@JsonProperty("topic") String topic,
                     @JsonProperty("partitionBy") String partitionBy) {
        this.topic = topic;

        if (partitionBy == null) {
            this.partitionBy = PARTITION_BY_KEY;
        } else {
            this.partitionBy = partitionBy;
        }
    }

    @JsonProperty
    public String getTopic() {
        return topic;
    }

    @JsonProperty
    public String getPartitionBy() {
        return partitionBy;
    }



    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append("topic: ").append(topic).append(", ")
                .append("partitionBy: ").append(partitionBy)
                .append("}");

        return builder.toString();
    }
}
