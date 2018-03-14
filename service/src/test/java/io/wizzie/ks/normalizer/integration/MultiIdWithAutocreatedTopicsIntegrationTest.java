package io.wizzie.ks.normalizer.integration;

import io.wizzie.ks.normalizer.builder.Builder;
import io.wizzie.ks.normalizer.builder.config.Config;
import io.wizzie.ks.normalizer.serializers.JsonDeserializer;
import io.wizzie.ks.normalizer.serializers.JsonSerde;
import io.wizzie.ks.normalizer.serializers.JsonSerializer;
import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.junit.Assert.assertEquals;

public class MultiIdWithAutocreatedTopicsIntegrationTest {

    private final static int NUM_BROKERS = 1;

    @ClassRule
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final static MockTime MOCK_TIME = CLUSTER.time;

    private static final int REPLICATION_FACTOR = 1;

    private static final String INPUT_TOPIC_X = "myapp_X_input1";
    private static final String INPUT_TOPIC_Y = "myapp_Y_input1";

    private static final String OUTPUT_TOPIC_X = "myapp_X_output1";

    private static final String OUTPUT_TOPIC_Y = "myapp_Y_output1";

    private static final String BOOTSTRAP_TOPIC = "__normalizer_bootstrap";

    private static Properties producerConfig = new Properties();

    private static Properties consumerConfig = new Properties();

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
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
    public void multiIdWithAutocreatedTopicsShouldWorks() throws Exception {
        Map<String, Object> streamsConfiguration = new HashMap<>();

        String appId = UUID.randomUUID().toString();
        streamsConfiguration.put(APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        Config config_X = new Config(streamsConfiguration);
        config_X.put("file.bootstraper.path", Thread.currentThread().getContextClassLoader().getResource("multi-id-integration-test.json").getFile());
        config_X.put(Config.ConfigProperties.BOOTSTRAPER_CLASSNAME, "io.wizzie.ks.normalizer.builder.bootstrap.FileBootstraper");
        config_X.put("metric.enabled", false);
        config_X.put("multi.id", true);
        config_X.put("application.id", "myapp_X");

        Config config_Y = config_X.clone();
        config_Y.put("application.id", "myapp_Y");

        Builder builder_X = new Builder(config_X);

        Builder builder_Y = new Builder(config_Y);

        Map<String, Object> b1_X = new HashMap<>();
        b1_X.put("C", 6000L);

        Map<String, Object> a1_X = new HashMap<>();
        a1_X.put("B", b1_X);

        Map<String, Object> message1_X = new HashMap<>();
        message1_X.put("A", a1_X);

        message1_X.put("timestamp", 1122334455L);

        KeyValue<String, Map<String, Object>> kvStream1_X = new KeyValue<>("KEY_X", message1_X);

        Map<String, Object> b2_X = new HashMap<>();
        b2_X.put("C", 9000L);

        Map<String, Object> a2_X = new HashMap<>();
        a2_X.put("B", b2_X);

        Map<String, Object> message2_X = new HashMap<>();
        message2_X.put("A", a2_X);

        message2_X.put("timestamp", 1122334655L);

        KeyValue<String, Map<String, Object>> kvStream2_X = new KeyValue<>("KEY_X", message2_X);

        Map<String, Object> b1_Y = new HashMap<>();
        b1_Y.put("C", 6000L);

        Map<String, Object> a1_Y = new HashMap<>();
        a1_Y.put("B", b1_Y);

        Map<String, Object> message1_Y = new HashMap<>();
        message1_Y.put("A", a1_Y);

        message1_Y.put("timestamp", 1122334455L);

        KeyValue<String, Map<String, Object>> kvStream1_Y = new KeyValue<>("KEY_Y", message1_Y);

        Map<String, Object> b2_Y = new HashMap<>();
        b2_Y.put("C", 9000L);

        Map<String, Object> a2_Y = new HashMap<>();
        a2_Y.put("B", b2_Y);

        Map<String, Object> message2_Y = new HashMap<>();
        message2_Y.put("A", a2_Y);

        message2_Y.put("timestamp", 1122334655L);

        KeyValue<String, Map<String, Object>> kvStream2_Y = new KeyValue<>("KEY_Y", message2_Y);


        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC_X, Arrays.asList(kvStream1_X, kvStream2_X), producerConfig, MOCK_TIME);
        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC_Y, Arrays.asList(kvStream1_Y, kvStream2_Y), producerConfig, MOCK_TIME);


        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("X", 6000);
        expectedData.put("timestamp", 1122334455);

        Map<String, Object> expectedData2 = new HashMap<>();
        expectedData2.put("X", 9000);
        expectedData2.put("timestamp", 1122334655);

        List<KeyValue> expectedResult_X = Arrays.asList(new KeyValue("KEY_X", expectedData), new KeyValue("KEY_X", expectedData2));

        List<KeyValue<String, Map>> receivedMessagesFromOutput_A = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC_X, 1);

        assertEquals(expectedResult_X, receivedMessagesFromOutput_A);

        List<KeyValue> expectedResult_Y = Arrays.asList(new KeyValue("KEY_Y", expectedData), new KeyValue("KEY_Y", expectedData2));

        List<KeyValue<String, Map>> receivedMessagesFromOutput_B = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC_Y, 1);

        assertEquals(expectedResult_Y, receivedMessagesFromOutput_B);
    }

    @AfterClass
    public static void stopKafkaCluster() {
        CLUSTER.stop();
    }


}
