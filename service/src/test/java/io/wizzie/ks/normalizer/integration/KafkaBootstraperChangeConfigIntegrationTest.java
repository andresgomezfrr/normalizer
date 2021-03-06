package io.wizzie.ks.normalizer.integration;

import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.ks.normalizer.builder.Builder;
import io.wizzie.ks.normalizer.base.builder.config.ConfigProperties;
import io.wizzie.ks.normalizer.serializers.JsonDeserializer;
import io.wizzie.ks.normalizer.serializers.JsonSerde;
import io.wizzie.ks.normalizer.serializers.JsonSerializer;
import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static io.wizzie.bootstrapper.bootstrappers.impl.KafkaBootstrapper.BOOTSTRAP_TOPICS_CONFIG;
import static org.junit.Assert.assertEquals;

public class KafkaBootstraperChangeConfigIntegrationTest {
    private final static int NUM_BROKERS = 1;

    @ClassRule
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final static MockTime MOCK_TIME = CLUSTER.time;
    private long waitTime = 60000;

    private static final int REPLICATION_FACTOR = 1;

    private static final String INPUT_TOPIC = "input1";

    private static final String OUTPUT_TOPIC1 = "output1";
    private static final String OUTPUT_TOPIC2 = "output2";

    private static final String BOOTSTRAP_TOPIC = "__normalizer_bootstrap";

    private static Properties producerConfig = new Properties();

    private static Properties consumerConfig = new Properties();

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(INPUT_TOPIC, 2, REPLICATION_FACTOR);
        CLUSTER.createTopic(OUTPUT_TOPIC1, 2, REPLICATION_FACTOR);
        CLUSTER.createTopic(OUTPUT_TOPIC2, 2, REPLICATION_FACTOR);

        CLUSTER.createTopic(BOOTSTRAP_TOPIC, 1, REPLICATION_FACTOR);


        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);


        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @Test
    public void kafkaBoostraperShouldWorkAfterChangeConfiguration() throws Exception {

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("kafka-bootstraper-integration-test-1.json").getFile());

        Map<String, Object> streamsConfiguration = new HashMap<>();

        String appId = UUID.randomUUID().toString();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        Config configuration = new Config(streamsConfiguration);
        configuration.put(ConfigProperties.BOOTSTRAPER_CLASSNAME, "io.wizzie.bootstrapper.bootstrappers.impl.KafkaBootstrapper");
        configuration.put(BOOTSTRAP_TOPICS_CONFIG, Arrays.asList(BOOTSTRAP_TOPIC));

        Builder builder = new Builder(configuration);

        String jsonConfig1 = getFileContent(file);

        KeyValue<String, String> jsonConfig1Kv = new KeyValue<>(appId, jsonConfig1);

        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        IntegrationTestUtils.produceKeyValuesSynchronously(BOOTSTRAP_TOPIC, Collections.singletonList(jsonConfig1Kv), producerConfig, MOCK_TIME);

        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<KeyValue<String, String>> receivedMessagesFromConfig1 = IntegrationTestUtils.waitUntilMinValuesRecordsReceived(consumerConfig, BOOTSTRAP_TOPIC, 1, waitTime);

        assertEquals(Collections.singletonList(jsonConfig1), receivedMessagesFromConfig1);

        Map<String, Object> b1 = new HashMap<>();
        b1.put("C", 6000L);

        Map<String, Object> a1 = new HashMap<>();
        a1.put("B", b1);

        Map<String, Object> message1 = new HashMap<>();
        message1.put("A", a1);

        message1.put("timestamp", 1122334455L);

        KeyValue<String, Map<String, Object>> kvStream1 = new KeyValue<>("KEY_A", message1);

        Map<String, Object> b2 = new HashMap<>();
        b2.put("C", 9000L);

        Map<String, Object> a2 = new HashMap<>();
        a2.put("B", b2);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("A", a2);

        message2.put("timestamp", 1122334655L);

        KeyValue<String, Map<String, Object>> kvStream2 = new KeyValue<>("KEY_A", message2);

        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, Arrays.asList(kvStream1, kvStream2), producerConfig, MOCK_TIME);

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("X", 3000);
        expectedData.put("timestamp", 1122334655);
        expectedData.put("last_timestamp", 1122334455);

        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>("KEY_A", expectedData);

        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        List<KeyValue<String, Map>> receivedMessagesFromOutput = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC1, 1, waitTime);

        assertEquals(Collections.singletonList(expectedDataKv), receivedMessagesFromOutput);

        // Second configuration

        file = new File(classLoader.getResource("kafka-bootstraper-integration-test-2.json").getFile());

        String jsonConfig2 = getFileContent(file);

        KeyValue<String, String> jsonConfig2Kv = new KeyValue<>(appId, jsonConfig2);

        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        IntegrationTestUtils.produceKeyValuesSynchronously(BOOTSTRAP_TOPIC, Collections.singletonList(jsonConfig2Kv), producerConfig, MOCK_TIME);

        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        Map<String, Object> message3 = new HashMap<>();
        message3.put("B", "VALUE-B");
        message3.put("timestamp", 1122334455);

        KeyValue<String, Map<String, Object>> kvStream3 = new KeyValue<>("KEY_B", message3);

        Map<String, Object> message4 = new HashMap<>();
        message4.put("A", 1000);
        message4.put("timestamp", 1122334555);
        message4.put("last_timestamp", 1122334355);

        KeyValue<String, Map<String, Object>> kvStream4 = new KeyValue<>("KEY_A", message4);

        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        Thread.sleep(5000); // Wait to load the new stream config
        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, Collections.singletonList(kvStream3), producerConfig, MOCK_TIME);

        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        List<KeyValue<String, Object>> receivedFilteredMessage = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC2, 1, waitTime);

        assertEquals(Collections.singletonList(kvStream3), receivedFilteredMessage);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, Collections.singletonList(kvStream4), producerConfig, MOCK_TIME);

        List<KeyValue<String, Object>> receivedSplitterMessage = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC1, 4, waitTime);

        List<KeyValue<String, Map<String, Object>>> expectedSplitterArray = new ArrayList<>();

        Map<String, Object> expectedMap = new HashMap<>();
        expectedMap.put("A", 125);
        expectedMap.put("timestamp", 1122334355);

        expectedSplitterArray.add(new KeyValue<>("KEY_A", expectedMap));

        expectedMap = new HashMap<>();
        expectedMap.put("A", 300);
        expectedMap.put("timestamp", 1122334380);

        expectedSplitterArray.add(new KeyValue<>("KEY_A", expectedMap));

        expectedMap = new HashMap<>();
        expectedMap.put("A", 300);
        expectedMap.put("timestamp", 1122334440);

        expectedSplitterArray.add(new KeyValue<>("KEY_A", expectedMap));

        expectedMap = new HashMap<>();
        expectedMap.put("A", 275);
        expectedMap.put("timestamp", 1122334500);

        expectedSplitterArray.add(new KeyValue<>("KEY_A", expectedMap));

        assertEquals(expectedSplitterArray, receivedSplitterMessage);

    }

    public static String getFileContent(File file) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));

        StringBuilder stringBuffer = new StringBuilder();

        String line;

        while ((line = bufferedReader.readLine()) != null) {

            stringBuffer.append(line).append("\n");
        }

        return stringBuffer.toString();
    }

}
