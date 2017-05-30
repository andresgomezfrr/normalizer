package io.wizzie.ks.normalizer.integration;


import io.wizzie.ks.normalizer.builder.Builder;
import io.wizzie.ks.normalizer.builder.config.Config;
import io.wizzie.ks.normalizer.serializers.JsonDeserializer;
import io.wizzie.ks.normalizer.serializers.JsonSerde;
import io.wizzie.ks.normalizer.serializers.JsonSerializer;
import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MultiIdWithAutocreatedTopicsIntegrationTest {
    private final static int NUM_BROKERS = 1;

    @ClassRule
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final static MockTime MOCK_TIME = CLUSTER.time;

    // Input topics
    private static final String INPUT_TOPIC_A = "myapp_A_input1";
    private static final String INPUT_TOPIC_B = "myapp_B_input1";

    // Output topics
    private static final String OUTPUT_TOPIC1_A = "myapp_A_output1";
    private static final String OUTPUT_TOPIC1_B = "myapp_B_output1";

    @Test
    public void multiIdShouldWork() throws Exception {
        // Stream configuration
        Map<String, Object> streamsConfiguration = new HashMap<>();
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        // Producer properties
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // Consumer properties
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Config for normalizer app A
        Config config_A = new Config(streamsConfiguration);
        config_A.put("file.bootstraper.path", Thread.currentThread().getContextClassLoader().getResource("multi-id-integration-test.json").getFile());
        config_A.put(Config.ConfigProperties.BOOTSTRAPER_CLASSNAME, "io.wizzie.ks.normalizer.builder.bootstrap.FileBootstraper");
        config_A.put("metric.enabled", false);
        config_A.put("multi.id", true);
        config_A.put("application.id", "myapp_A");

        // Config for normalizer app B
        Config config_B = config_A.clone();
        config_B.put("application.id", "myapp_B");

        // Create first message : {"A": {"B": {"C": 6000}, "timestamp": 1122334455}
        Map<String, Object> message1_b = new HashMap<>();
        message1_b.put("C", 6000L);

        Map<String, Object> message1_a = new HashMap<>();
        message1_a.put("B", message1_b);

        Map<String, Object> message1 = new HashMap<>();
        message1.put("A", message1_a);
        message1.put("timestamp", 1122334455L);

        // Create second message : {"A": {"B": {"C": 9000}, "timestamp": 1122334655}
        Map<String, Object> message2_b = new HashMap<>();
        message2_b.put("C", 9000L);

        Map<String, Object> message2_a = new HashMap<>();
        message2_a.put("B", message2_b);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("A", message2_a);

        message2.put("timestamp", 1122334655L);

        // Create KeyValue object from both message1 and message2
        KeyValue<String, Map<String, Object>> kvStream1_A = new KeyValue<>("KEY_A", message1);
        KeyValue<String, Map<String, Object>> kvStream2_A = new KeyValue<>("KEY_A", message2);

        KeyValue<String, Map<String, Object>> kvStream1_B = new KeyValue<>("KEY_B", message1);
        KeyValue<String, Map<String, Object>> kvStream2_B = new KeyValue<>("KEY_B", message2);

        // Expected message : {"X": 6000, "timestmap": 1122334455}
        Map<String, Object> expectedData1 = new HashMap<>();
        expectedData1.put("X", 6000);
        expectedData1.put("timestamp", 1122334455);

        // Expected message : {"X": 9000, "timestmap": 1122334655}
        Map<String, Object> expectedData2 = new HashMap<>();
        expectedData2.put("X", 9000);
        expectedData2.put("timestamp", 1122334655);

        // Step 1 produce key value messages synchronously
        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC_A, Arrays.asList(kvStream1_A, kvStream2_A), producerConfig, MOCK_TIME);
        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC_B, Arrays.asList(kvStream1_B, kvStream2_B), producerConfig, MOCK_TIME);

        // Step 2 start normalizer apps
        Builder builder_A = new Builder(config_A);
        Builder builder_B = new Builder(config_B);

        // Step 3 consumer results and check them
        List<KeyValue> expectedResult_A = Arrays.asList(new KeyValue("KEY_A", expectedData1), new KeyValue("KEY_A", expectedData2));
        List<KeyValue<String, Map>> receivedMessagesFromOutput_A = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC1_A, 1);
        assertEquals(expectedResult_A, receivedMessagesFromOutput_A);

        List<KeyValue> expectedResult_B = Arrays.asList(new KeyValue("KEY_B", expectedData1), new KeyValue("KEY_B", expectedData2));
        List<KeyValue<String, Map>> receivedMessagesFromOutput_B = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC1_B, 1);

        assertEquals(expectedResult_B, receivedMessagesFromOutput_B);

        // Close normalizer apps
        builder_A.close();
        builder_B.close();
    }

    @AfterClass
    public static void stopKafkaCluster() {
        CLUSTER.stop();
    }

}
