package rb.ks.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class StreamModel {
    private List<FunctionModel> funcs;
    private List<SinkModel> sinks;

    @JsonCreator
    public StreamModel(
            @JsonProperty("funcs") List<FunctionModel> funcs,
            @JsonProperty("sinks") List<SinkModel> sinks) {
        this.funcs = funcs;
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


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append("funcs: ").append(funcs).append(", ")
                .append("sinks: ").append(sinks)
                .append("}");

        return builder.toString();
    }
}
