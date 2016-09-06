package zz.ks.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class StreamModel {
    List<MapperModel> mappers;
    List<SinkModel> sinks;

    @JsonCreator
    public StreamModel(
            @JsonProperty("mappers") List<MapperModel> mappers,
            @JsonProperty("sinks") List<SinkModel> sinks) {
        this.mappers = mappers;
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

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append("mappers: ").append(mappers).append(", ")
                .append("sinks: ").append(sinks)
                .append("}");

        return builder.toString();
    }
}
