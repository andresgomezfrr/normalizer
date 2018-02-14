package rb.ks.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class StreamModel {
    List<FunctionModel> funcs;
    TimestamperModel timestamper;
    List<SinkModel> sinks;

    @JsonCreator
    public StreamModel(
            @JsonProperty("funcs") List<FunctionModel> funcs,
            @JsonProperty("timestamper") TimestamperModel timestamper,
            @JsonProperty("sinks") List<SinkModel> sinks) {
        this.funcs = funcs;
        this.timestamper = timestamper;
        this.sinks = sinks;
    }

    @JsonProperty
    public List<FunctionModel> getFuncs() {
        return funcs;
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
                .append("funcs: ").append(funcs).append(", ")
                .append("timestamper: ").append(timestamper).append(", ")
                .append("sinks: ").append(sinks)
                .append("}");

        return builder.toString();
    }
}
