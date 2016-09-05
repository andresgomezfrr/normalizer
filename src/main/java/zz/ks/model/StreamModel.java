package zz.ks.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class StreamModel {
    public List<MapperModel> mappers;
    List<OutputModel> outputs;

    @JsonCreator
    public StreamModel(
            @JsonProperty("mappers") List<MapperModel> mappers,
            @JsonProperty("outputs") List<OutputModel> outputs) {
        this.mappers = mappers;
        this.outputs = outputs;
    }

    @JsonProperty
    public List<MapperModel> getMappers() {
        return mappers;
    }


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append("mappers: ").append(mappers).append(", ")
                .append("outputs: ").append(outputs)
                .append("}");

        return builder.toString();
    }
}
