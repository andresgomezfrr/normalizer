package zz.ks;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import zz.ks.exceptions.PlanBuilderException;
import zz.ks.model.PlanModel;
import zz.ks.serializers.JsonSerde;

import java.io.IOException;
import java.util.Properties;

public class Normalizer {
    public static void main(String[] args) throws IOException, PlanBuilderException {
        String json = "{\n" +
                "  \"inputs\":{\n" +
                "    \"topic1\":[\"stream1\", \"stream2\"]\n" +
                "  },\n" +
                "  \"streams\":{\n" +
                "    \"stream1\":{\n" +
                "        \"mappers\":[\n" +
                "                    {\"dimPath\":[\"A\",\"B\",\"C\"], \"as\":\"X\"},\n" +
                "                    {\"dimPath\":[\"Y\",\"W\",\"Z\"], \"as\":\"Q\"},\n" +
                "                    {\"dimPath\":[\"timestamp\"]}\n" +
                "                  ],\n" +
                "        \"timestamper\":{\"dimension\":\"timestamp\", \"format\":\"iso\"},\n" +
                "        \"sinks\":[\n" +
                "            {\"topic\":\"output\", \"partitionBy\":\"Q\"},\n" +
                "            {\"topic\":\"output1\"}\n" +
                "        ]\n" +
                "    },\n" +
                "    \"stream2\":{\n" +
                "        \"mappers\":[\n" +
                "                    {\"dimPath\":[\"A\",\"B\",\"C\"], \"as\":\"T\"},\n" +
                "                    {\"dimPath\":[\"Y\",\"W\",\"Z\"], \"as\":\"R\"},\n" +
                "                    {\"dimPath\":[\"timestamp_ms\"], \"as\":\"timestamp\"}\n" +
                "                  ],\n" +
                "        \"timestamper\":{\"dimension\":\"timestamp\", \"format\":\"ms\"},\n" +
                "        \"sinks\":[\n" +
                "            {\"topic\":\"output\", \"partitionBy\":\"T\"}\n" +
                "        ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "ks-normalizer");
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        ObjectMapper objectMapper = new ObjectMapper();

        PlanModel model = objectMapper.readValue(json, PlanModel.class);
        StreamBuilder streamBuilder = new StreamBuilder();

        KafkaStreams streams = new KafkaStreams(streamBuilder.builder(model), streamsConfiguration);
        streams.start();
    }
}
