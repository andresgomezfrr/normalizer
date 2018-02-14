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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import rb.ks.builder.StreamBuilder;
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

public class StreamsIntegrationTest {

    @ClassRule
    public static EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    private static final int REPLICATION_FACTOR = 1;

    private static final String INPUT_TOPIC = "input1";

    private static final String OUTPUT_TOPIC_1 = "output1";
    private static final String OUTPUT_TOPIC_2 = "output2";

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        // inputs
        CLUSTER.createTopic(INPUT_TOPIC, 2, REPLICATION_FACTOR);

        // sinks
        CLUSTER.createTopic(OUTPUT_TOPIC_1, 2, REPLICATION_FACTOR);
        CLUSTER.createTopic(OUTPUT_TOPIC_2, 2, REPLICATION_FACTOR);
    }

    @Test
    public void streamsShouldWorkWithKafka() throws InterruptedException, PlanBuilderException {

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("streams-integration-test.json").getFile());

        Properties streamsConfiguration = new Properties();

        String appId = UUID.randomUUID().toString();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ObjectMapper mapper = new ObjectMapper();

        PlanModel model = null;

        try {
            model = mapper.readValue(file, PlanModel.class);
        } catch (IOException e) {
            fail("Exception : " + e.getMessage());
        }

        StreamBuilder streamBuilder = new StreamBuilder(appId);

        KafkaStreams streams = new KafkaStreams(streamBuilder.builder(model), streamsConfiguration);

        streams.start();

        Map<String, Object> subMessage = new HashMap<>();
        subMessage.put("B", "VALUE-B");

        Map<String, Object> message1 = new HashMap<>();

        message1.put("A", subMessage);
        message1.put("timestamp", 1122334455L);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("C", "VALUE-C");
        message2.put("D", 200000L);
        message2.put("timestamp", 1122334755L);

        Map<String, Object> message3 = new HashMap<>();
        message3.put("C", "VALUE-C");
        message3.put("D", 400000L);
        message3.put("timestamp", 1122334955L);

        KeyValue<String, Map<String, Object>> kvStreams1 = new KeyValue<>("KEY_1", message1);
        KeyValue<String, Map<String, Object>> kvStreams2 = new KeyValue<>("KEY_2", message2);
        KeyValue<String, Map<String, Object>> kvStreams3 = new KeyValue<>("KEY_2", message3);


        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        try {
            IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, Arrays.asList(kvStreams1, kvStreams2, kvStreams3), producerConfig);
        } catch (ExecutionException e) {
            fail("Exception : " + e.getMessage());
        }

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-streams-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfigA.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Properties consumerConfigB = new Properties();
        consumerConfigB.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigB.put(ConsumerConfig.GROUP_ID_CONFIG, "test-streams-consumer-B");
        consumerConfigB.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigB.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfigB.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<KeyValue<String, Map>> result1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, OUTPUT_TOPIC_1, 1);

        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("X", "VALUE-B");
        expectedMessage1.put("timestamp", 1122334455);

        KeyValue<String, Map<String, Object>> kvExpected1 = new KeyValue<>("KEY_1", expectedMessage1);

        assertEquals(Collections.singletonList(kvExpected1), result1);

        List<KeyValue<String, Map>> result2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigB, OUTPUT_TOPIC_2, 1);

        ArrayList<KeyValue<String, Map<String, Object>>> kvExcepted2 = new ArrayList<>();

        Map<String, Object> expectedMessage = new HashMap<>();
        expectedMessage.put("C", "VALUE-C");
        expectedMessage.put("timestamp", 1122334755);

        kvExcepted2.add(new KeyValue<>("KEY_2", expectedMessage));

        expectedMessage = new HashMap<>();
        expectedMessage.put("C", "VALUE-C");
        expectedMessage.put("D", 45000);
        expectedMessage.put("timestamp", 1122334755);

        kvExcepted2.add(new KeyValue<>("KEY_2", expectedMessage));

        expectedMessage = new HashMap<>();
        expectedMessage.put("C", "VALUE-C");
        expectedMessage.put("D", 60000);
        expectedMessage.put("timestamp", 1122334800);

        kvExcepted2.add(new KeyValue<>("KEY_2", expectedMessage));

        expectedMessage = new HashMap<>();
        expectedMessage.put("C", "VALUE-C");
        expectedMessage.put("D", 60000);
        expectedMessage.put("timestamp", 1122334860);

        kvExcepted2.add(new KeyValue<>("KEY_2", expectedMessage));

        expectedMessage = new HashMap<>();
        expectedMessage.put("C", "VALUE-C");
        expectedMessage.put("D", 35000);
        expectedMessage.put("timestamp", 1122334920);

        kvExcepted2.add(new KeyValue<>("KEY_2", expectedMessage));

        assertEquals(kvExcepted2, result2);

        streams.close();
        streamBuilder.close();

    }

    @AfterClass
    public static void stop(){
        CLUSTER.stop();
    }


}
