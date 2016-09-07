package rb.ks.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class StreamModel {
    List<MapperModel> mappers;
    TimestamperModel timestamper;
    List<SinkModel> sinks;

    @JsonCreator
    public StreamModel(
            @JsonProperty("mappers") List<MapperModel> mappers,
            @JsonProperty("timestamper") TimestamperModel timestamper,
            @JsonProperty("sinks") List<SinkModel> sinks) {
        this.mappers = mappers;
        this.timestamper = timestamper;
        this.sinks = sinks;
    }

    @JsonProperty
    public List<MapperModel> getMappers() {
        return mappers;
    }

    @JsonProperty
    public List<SinkModel> getSinks() {
        return sinks;
    }

    @JsonProperty
    public TimestamperModel getTimestamper() {
        return timestamper;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append("mappers: ").append(mappers).append(", ")
                .append("timestamper: ").append(timestamper).append(", ")
                .append("sinks: ").append(sinks)
                .append("}");

        return builder.toString();
    }
}
