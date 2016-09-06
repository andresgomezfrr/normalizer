package zz.ks.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import zz.ks.exceptions.PlanBuilderException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
        validateTimestamper();
        validateSinks();
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
        for (String stream : streams.keySet()) {
            if (!definedStreams.contains(stream)) {
                throw new PlanBuilderException(String.format("Stream[%s]: Not defined on inputs. Available inputs %s", stream, inputs));
            }
        }
    }

    private void validateTimestamper() throws PlanBuilderException {
        for (Map.Entry<String, StreamModel> stream : streams.entrySet()) {
            Set<String> dimensions = stream.getValue().getMappers().stream().map(mapper -> mapper.as).collect(Collectors.toSet());
            TimestamperModel timestamper = stream.getValue().getTimestamper();

            if (!dimensions.contains(timestamper.getTimestampDim()) && !timestamper.getFormat().equals("generate")) {
                throw new PlanBuilderException(String.format("Stream[%s]:" +
                                " Timestamp dimension [%s] is not on the message. Available dimensions %s",
                        stream.getKey(), timestamper.getTimestampDim(), dimensions));
            }
        }
    }

    private void validateSinks() throws PlanBuilderException {
        for (Map.Entry<String, StreamModel> stream : streams.entrySet()) {
            Set<String> dimensions = stream.getValue().getMappers().stream().map(mapper -> mapper.as).collect(Collectors.toSet());

            for (SinkModel sink : stream.getValue().getSinks()) {
                if (!sink.getPartitionBy().equals(SinkModel.PARTITION_BY_KEY) && !dimensions.contains(sink.getPartitionBy())) {
                    throw new PlanBuilderException(String.format("Stream[%s]:" +
                                    " PartitionBy dimension [%s] is not on the message. Available dimensions %s",
                            stream.getKey(), sink.getPartitionBy(), dimensions));
                }
            }
        }
    }
}
