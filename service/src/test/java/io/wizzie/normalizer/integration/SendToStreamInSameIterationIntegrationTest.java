package io.wizzie.normalizer.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.normalizer.builder.StreamBuilder;
import io.wizzie.normalizer.exceptions.PlanBuilderException;
import io.wizzie.normalizer.model.PlanModel;
import io.wizzie.normalizer.serializers.JsonDeserializer;
import io.wizzie.normalizer.serializers.JsonSerde;
import io.wizzie.normalizer.serializers.JsonSerializer;
import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SendToStreamInSameIterationIntegrationTest {
    private final static int NUM_BROKERS = 1;

    @ClassRule
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final static MockTime MOCK_TIME = CLUSTER.time;

    private static final int REPLICATION_FACTOR = 1;

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        // inputs
        CLUSTER.createTopic("input", 2, REPLICATION_FACTOR);

        // sinks
        CLUSTER.createTopic("output", 2, REPLICATION_FACTOR);
    }

    @Test
    public void sendToSameStreamShouldWork() throws InterruptedException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("loop-to-stream-in-same-iteration.json").getFile());

        Properties streamsConfiguration = new Properties();

        String appId = UUID.randomUUID().toString();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        ObjectMapper objectMapper = new ObjectMapper();

        PlanModel model = null;

        try {
            model = objectMapper.readValue(file, PlanModel.class);
        } catch (IOException e) {
            fail("Exception : " + e.getMessage());
        }

        Config config = new Config();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");

        StreamBuilder streamBuilder = new StreamBuilder(config, null);

        KafkaStreams streams = null;

        try {
            streams = new KafkaStreams(streamBuilder.builder(model).build(), streamsConfiguration);
        } catch (PlanBuilderException e) {
            fail("Exception : " + e.getMessage());
        }

        streams.start();

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 1473316426);
        message1.put("fieldA", "valueA");
        message1.put("dim2", null);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("timestamp", 1473316426);
        message2.put("fieldA", "valueA");
        message2.put("dim2", "value2");

        KeyValue<String, Map<String, Object>> kvStream1 = new KeyValue<>("KEY_A", message1);
        KeyValue<String, Map<String, Object>> kvStream2 = new KeyValue<>("KEY_A", message2);

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        try {
            IntegrationTestUtils.produceKeyValuesSynchronously("input", Collections.singletonList(kvStream1), producerConfig, MOCK_TIME);
            IntegrationTestUtils.produceKeyValuesSynchronously("input", Collections.singletonList(kvStream2), producerConfig, MOCK_TIME);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        Map<String, Object> expectedData1 = new HashMap<>();
        expectedData1.put("timestamp", 1473316426);
        expectedData1.put("fieldA", "valueA");
        expectedData1.put("dim2", "stream2");
        expectedData1.put("dim1", "stream1");

        Map<String, Object> expectedData2 = new HashMap<>();
        expectedData2.put("timestamp", 1473316426);
        expectedData2.put("fieldA", "valueA");
        expectedData2.put("dim2", "value2");
        expectedData2.put("dim1", "stream1");

        KeyValue<String, Map<String, Object>> expectedDataKv1 = new KeyValue<>("KEY_A", expectedData1);
        KeyValue<String, Map<String, Object>> expectedDataKv2 = new KeyValue<>("KEY_A", expectedData2);

        List<KeyValue<String, Map>> receivedMessagesFromOutput = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "output", 2);

        assertEquals(Collections.singletonList(expectedDataKv1), Collections.singletonList(receivedMessagesFromOutput.get(0)));
        assertEquals(Collections.singletonList(expectedDataKv2), Collections.singletonList(receivedMessagesFromOutput.get(1)));

        streams.close();
        streamBuilder.close();
    }
}
