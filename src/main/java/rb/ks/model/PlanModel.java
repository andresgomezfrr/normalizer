package rb.ks.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import rb.ks.exceptions.PlanBuilderException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PlanModel {
    Map<String, List<String>> inputs;
    Map<String, StreamModel> streams;
    List<String> definedStreams = new ArrayList<>();

    @JsonCreator
    public PlanModel(@JsonProperty("inputs") Map<String, List<String>> inputs,
                     @JsonProperty("streams") Map<String, StreamModel> streams) {
        this.inputs = inputs;
        this.streams = streams;
    }

    @JsonProperty
    public Map<String, List<String>> getInputs() {
        return inputs;
    }

    @JsonProperty
    public Map<String, StreamModel> getStreams() {
        return streams;
    }

    public List<String> getDefinedStreams() {
        return definedStreams;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append("inputs: ").append(inputs).append(", ")
                .append("streams: ").append(streams)
                .append("}");

        return builder.toString();
    }

    public void validate() throws PlanBuilderException {
        validateInputs();
        validateStreams();
    }

    private void validateInputs() throws PlanBuilderException {
        for (List<String> streams : inputs.values()) {
            for (String stream : streams) {
                if (definedStreams.contains(stream)) {
                    definedStreams.clear();
                    throw new PlanBuilderException(String.format("Stream[%s]: Duplicated", stream));
                } else {
                    definedStreams.add(stream);
                }
            }
        }
    }


    private void validateStreams() throws PlanBuilderException {
        for (Map.Entry<String, StreamModel> entry : streams.entrySet()) {
            List<SinkModel> sinks = entry.getValue().sinks;
            if (sinks != null) {
                for (SinkModel sink : sinks) {
                    if (sink.getType().equals(SinkModel.STREAM_TYPE)) {
                        definedStreams.add(sink.getTopic());
                    }
                }

                if (!definedStreams.contains(entry.getKey())) {
                    throw new PlanBuilderException(String.format("Stream[%s]: Not defined on inputs. Available definedStreams %s", entry.getKey(), definedStreams));
                }
            }
        }
    }
}
