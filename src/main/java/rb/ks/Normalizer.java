package rb.ks;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rb.ks.exceptions.PlanBuilderException;
import rb.ks.model.PlanModel;
import rb.ks.serializers.JsonSerde;

import java.io.IOException;
import java.util.Properties;

public class Normalizer {
    private static final Logger log = LoggerFactory.getLogger(Normalizer.class);

    public static void main(String[] args) throws IOException, PlanBuilderException {


        String json = "{\n" +
                "  \"inputs\":{\n" +
                "    \"topic1\":[\"stream1\"]\n" +
                "  },\n" +
                "  \"streams\":{\n" +
                "    \"stream1\":{\n" +
                "        \"funcs\":[\n" +
                "              {\n" +
                "                \"name\":\"myMapper\",\n" +
                "                \"className\":\"rb.ks.funcs.SimpleMapper\",\n" +
                "                \"properties\": {\n" +
                "                  \"maps\": [\n" +
                "                    {\"dimPath\":[\"A\",\"B\",\"C\"], \"as\":\"X\"},\n" +
                "                    {\"dimPath\":[\"Y\",\"W\",\"Z\"], \"as\":\"Q\"}, \n" +
                "                    {\"dimPath\":[\"timestamp\"]}\n" +
                "                  ]\n" +
                "                }\n" +
                "              },\n" +
                "              {\n" +
                "                \"name\":\"diffCounterMapper\",\n" +
                "                \"className\":\"rb.ks.funcs.DiffCounterStoreMapper\",\n" +
                "                \"properties\": {\n" +
                "                  \"counters\": [\"X\"]\n" +
                "                },\n" +
                "                \"stores\":[\"counter-store\"]\n" +
                "              }\n" +
                "        ],\n" +
                "        \"timestamper\":{\"dimension\":\"timestamp\", \"format\":\"generate\"},\n" +
                "        \"sinks\":[\n" +
                "            {\"topic\":\"output\", \"partitionBy\":\"Q\"},\n" +
                "            {\"topic\":\"output1\"}\n" +
                "        ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "ks-normalizer-5");
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        ObjectMapper objectMapper = new ObjectMapper();

        PlanModel model = objectMapper.readValue(json, PlanModel.class);
        System.out.println(model.toString());
        StreamBuilder streamBuilder = new StreamBuilder();

        KafkaStreams streams = new KafkaStreams(streamBuilder.builder(model), streamsConfiguration);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streamBuilder.close();
            streams.close();
        }));


        log.info("Started Normalizer with conf {}", streamsConfiguration);
    }
}
