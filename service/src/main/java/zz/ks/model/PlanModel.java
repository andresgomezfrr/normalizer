package zz.ks.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import zz.ks.exceptions.PlanBuilderException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PlanModel {
    private Map<String, List<String>> inputs;
    private Map<String, StreamModel> streams;
    private List<String> definedStreams = new ArrayList<>();

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

    public String printExecutionPlan() {
        ObjectMapper mapper = new ObjectMapper();
        StringBuilder builder = new StringBuilder();
        StringBuilder propertiesBuilder = new StringBuilder();
        propertiesBuilder.append("\n").append("Properties: ").append("\n");

        inputs.entrySet().forEach(inputEntry -> {
            builder.append("\n");
            builder.append("CREATE STREAMS ")
                    .append(inputEntry.getValue())
                    .append(" FROM KAFKA ")
                    .append(inputEntry.getKey());
        });
        builder.append("\n");
        streams.entrySet().forEach(entry -> {
            builder.append("\n");

            builder.append("FROM ").append(entry.getKey()).append("\n");

            List<FunctionModel> funcs = entry.getValue().getFuncs();
            if(funcs != null) {
                List<String> funcNames = funcs.stream()
                        .map(FunctionModel::getName).collect(Collectors.toList());
                builder.append("   TRANSFORM USING ").append(funcNames).append("\n");

                funcs.forEach(func -> {
                    propertiesBuilder.append("   * ").append(func.getName()).append(": ");

                    try {
                        propertiesBuilder.append(mapper.writeValueAsString(func.getProperties())).append("\n");
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                });
            }

            List<SinkModel> kafkaTopics = entry.getValue().getSinks().stream()
                    .filter(sink -> sink.getType().equals(SinkModel.KAFKA_TYPE))
                    .collect(Collectors.toList());

            List<SinkModel> streamNames = entry.getValue().getSinks().stream()
                    .filter(sink -> sink.getType().equals(SinkModel.STREAM_TYPE))
                    .collect(Collectors.toList());


            kafkaTopics.forEach(sink -> {
                builder.append("   SEND TO KAFKA ").append(sink.getTopic())
                        .append(" PARTITION BY ").append(sink.getPartitionBy());

                if (sink.getFilter() != null) {
                    builder.append(" FILTER WITH ").append(sink.getFilter().getName());
                    propertiesBuilder.append("   * ").append(sink.getFilter().getName()).append(": ");
                    try {
                        propertiesBuilder
                                .append(mapper.writeValueAsString(sink.getFilter().getProperties())).append("\n");
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                }

                builder.append("\n");
            });

            streamNames.forEach(sink -> {
                builder.append("   SEND TO STREAM ").append(sink.getTopic())
                        .append(" PARTITION BY ").append(sink.getPartitionBy());

                if (sink.getFilter() != null) {
                    builder.append(" FILTER WITH ").append(sink.getFilter().getName());
                    propertiesBuilder.append("   * ").append(sink.getFilter().getName()).append(": ");
                    try {
                        propertiesBuilder
                                .append(mapper.writeValueAsString(sink.getFilter().getProperties())).append("\n");
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                }
                builder.append("\n");
            });
        });

        builder.append(propertiesBuilder.toString());
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
            List<SinkModel> sinks = entry.getValue().getSinks();
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
