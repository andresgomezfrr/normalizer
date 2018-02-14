package integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import rb.ks.StreamBuilder;
import rb.ks.exceptions.PlanBuilderException;
import rb.ks.model.PlanModel;
import rb.ks.serializers.JsonDeserializer;
import rb.ks.serializers.JsonSerde;
import rb.ks.serializers.JsonSerializer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class KafkaIntegrationTest {

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();
    private static final int REPLICATION_FACTOR = 1;

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        // inputs
        CLUSTER.createTopic("stream1", 2, REPLICATION_FACTOR);
        CLUSTER.createTopic("stream2", 2, REPLICATION_FACTOR);

        // sinks
        CLUSTER.createTopic("output1", 4, REPLICATION_FACTOR);
        CLUSTER.createTopic("output2", 4, REPLICATION_FACTOR);
    }

    @Test
    public void shouldWorkTest() throws InterruptedException {

        String json = "{\n" +
                "  \"inputs\":{\n" +
                "    \"topic1\":[\"stream1\"]\n" +
                "  },\n" +
                "  \"streams\":{\n" +
                "    \"stream1\":{\n" +
                "        \"mappers\":[\n" +
                "                    {\"dimPath\":[\"A\",\"B\",\"C\"], \"as\":\"X\"},\n" +
                "                    {\"dimPath\":[\"timestamp\"]}\n" +
                "                  ],\n" +
                "        \"timestamper\":{\"dimension\":\"timestamp\", \"format\":\"iso\"},\n" +
                "        \"sinks\":[\n" +
                "            {\"topic\":\"output1\", \"partitionBy\":\"X\"}\n" +
                "        ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "ks-normalizer");
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        ObjectMapper objectMapper = new ObjectMapper();

        PlanModel model = null;

        try {
            model = objectMapper.readValue(json, PlanModel.class);
        } catch (IOException e) {
            fail("Exception : " + e.getMessage());
        }

        StreamBuilder streamBuilder = new StreamBuilder();

        KafkaStreams streams = null;
        try {
            streams = new KafkaStreams(streamBuilder.builder(model), streamsConfiguration);
        } catch (PlanBuilderException e) {
            fail("Exception : " + e.getMessage());
        }

        streams.start();

        String json2 = "{\"A\":{\"B\":{\"C\":\"PEPE\"}},\"timestamp\":\"2016-09-08T06:33:46.000Z\"}";

        KeyValue<String, Map<String, Object>> kvStream1 = null;

        try {
            kvStream1 = new KeyValue<>("PEPE", objectMapper.readValue(json2, Map.class));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        try {
            IntegrationTestUtils.produceKeyValuesSynchronously("stream1", Collections.singletonList(kvStream1), producerConfig);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        Properties consumerConfigB = new Properties();
        consumerConfigB.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigB.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-B");
        consumerConfigB.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfigB.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigB.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        List<KeyValue<String, Map>> receivedMessagesFromStream1 = IntegrationTestUtils.readKeyValues("stream1", consumerConfigA, 1);

        System.out.println(receivedMessagesFromStream1);

        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA,"output1", 1);

        System.out.println(receivedMessagesFromOutput1);

    }

}
