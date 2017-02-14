package rb.ks.integration;

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
import org.mockito.Mockito;
import rb.ks.StreamBuilder;
import rb.ks.exceptions.PlanBuilderException;
import rb.ks.model.PlanModel;
import rb.ks.serializers.JsonDeserializer;
import rb.ks.serializers.JsonSerde;
import rb.ks.serializers.JsonSerializer;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;

public class SimpleMapperIntegrationTest {

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    private static final int REPLICATION_FACTOR = 1;

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        // inputs
        CLUSTER.createTopic("input1", 2, REPLICATION_FACTOR);

        // sinks
        CLUSTER.createTopic("output1", 4, REPLICATION_FACTOR);
    }

    @Test
    public void simpleMapperShouldWork() throws InterruptedException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("simple-mapper-integration.json").getFile());

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
            model = Mockito.spy(objectMapper.readValue(file, PlanModel.class));
        } catch (IOException e) {
            fail("Exception : " + e.getMessage());
        }

        StreamBuilder streamBuilder = Mockito.spy(new StreamBuilder());

        KafkaStreams streams = null;

        try {
            streams = new KafkaStreams(streamBuilder.builder(model), streamsConfiguration);
        } catch (PlanBuilderException e) {
            fail("Exception : " + e.getMessage());
        }

        // Get inputs?
        Mockito.verify(model).getInputs();
        // Call getStreams method three times? (addSinks, addTimeStamper, createFuncs)
        Mockito.verify(model, times(3)).getStreams();

        streams.start();

        String jsonData = "{\"A\":{\"B\":{\"C\":\"VALUE\"}},\"timestamp\":\"2016-09-08T06:33:46.000Z\"}";

        KeyValue<String, Map<String, Object>> kvStream1 = null;

        try {
            kvStream1 = new KeyValue<>("KEY_A", objectMapper.readValue(jsonData, Map.class));
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
            IntegrationTestUtils.produceKeyValuesSynchronously("input1", Collections.singletonList(kvStream1), producerConfig);
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

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("X", "VALUE");
        expectedData.put("timestamp", 1473316426);

        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>("VALUE", expectedData);

        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "output1", 1);

        assertEquals(receivedMessagesFromOutput1, Collections.singletonList(expectedDataKv));
    }

}
