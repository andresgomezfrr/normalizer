package zz.ks.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

public class TimestampModel {
    public String timestampDim;
    public TimestampType type;

    @JsonCreator
    public TimestampModel(@JsonProperty("dimension") String timestampDim,
                          @JsonProperty("type") TimestampType type) {
        this.timestampDim = timestampDim;
        this.type = type;
    }


    @JsonProperty
    public String getTimestampDim() {
        return timestampDim;
    }

    @JsonProperty
    public TimestampType getType() {
        return type;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append("timestampDim: ").append(timestampDim).append(", ")
                .append("type: ").append(type.type)
                .append("}");

        return builder.toString();
    }

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    public enum TimestampType {
        SEC("sec"), MS("ms"), ISO("iso");

        public String type;

        TimestampType(String type) {
            this.type = type.toLowerCase();
        }

        @JsonValue
        public String getType() {
            return type;
        }

    }
}
