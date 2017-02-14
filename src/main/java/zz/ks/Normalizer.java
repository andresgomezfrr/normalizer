package zz.ks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import zz.ks.exceptions.PlanBuilderException;
import zz.ks.model.MapperModel;
import zz.ks.model.PlanModel;
import zz.ks.model.StreamModel;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Normalizer {
    public static void main(String[] args) throws IOException, PlanBuilderException {
        String json = "{\n" +
                "  \"inputs\":{\n" +
                "    \"topic1\":[\"stream1\", \"stream2\"],\n" +
                "    \"topic2\":[\"stream3\"]\n" +
                "  },\n" +
                "  \"streams\":{\n" +
                "    \"stream1\":{\n" +
                "        \"mappers\":[\n" +
                "                    {\"dimPath\":[\"A\",\"B\",\"C\"], \"as\":\"X\"},\n" +
                "                    {\"dimPath\":[\"Y\",\"W\",\"Z\"], \"as\":\"Q\"},\n" +
                "                    {\"dimPath\":[\"timestamp\"], \"as\":\"timestamp\"}\n" +
                "                  ],\n" +
                "        \"outputs\":[\n" +
                "            {\"topic\":\"output\", \"partitionBy\":\"Q\", \"timestamp\":{\"dimension\":\"timestamp\", \"type\":\"sec\"}}\n" +
                "        ]\n" +
                "    },\n" +
                "    \"stream2\":{\n" +
                "        \"mappers\":[\n" +
                "                    {\"dimPath\":[\"J\",\"L\",\"P\"], \"as\":\"R\"},\n" +
                "                    {\"dimPath\":[\"Y\",\"W\",\"Z\"], \"as\":\"Q\"},\n" +
                "                    {\"dimPath\":[\"R\", \"time\"], \"as\":\"timestamp\"}\n" +
                "                  ],\n" +
                "        \"outputs\":[\n" +
                "            {\"topic\":\"output\", \"partitionBy\":\"X\", \"timestamp\":{\"dimension\":\"timestamp\", \"type\":\"iso\"}}\n" +
                "        ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();

        PlanModel model = objectMapper.readValue(json, PlanModel.class);

        model.validate();

        KStreamBuilder builder = new KStreamBuilder();
        Map<String, KStream<String, ObjectNode>> kstreams = new HashMap<>();

        for (Map.Entry<String, List<String>> inputs : model.getInputs().entrySet()) {
            String topic = inputs.getKey();
            KStream<String, ObjectNode> kstream = builder.stream(topic);
            for (String stream : inputs.getValue()) kstreams.put(stream, kstream);
        }


        for (Map.Entry<String, StreamModel> streams : model.getStreams().entrySet()) {
            KStream<String, ObjectNode> kstream = kstreams.get(streams.getKey());

            //TODO: Do the mapper phase.

        }

        System.out.println(kstreams);
    }
}
