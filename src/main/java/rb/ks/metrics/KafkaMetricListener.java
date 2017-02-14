package rb.ks.metrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;
import rb.ks.builder.config.Config;

import java.util.HashMap;
import java.util.Map;

public class KafkaMetricListener implements MetricListener {
    public static final String METRIC_KAFKA_TOPIC = "metric.kafka.topic";
    KafkaProducer<String, Map<String, Object>> kafkaProducer;
    String topic;
    String appId;

    @Override
    public void init(Config config) {
        appId = config.get(StreamsConfig.APPLICATION_ID_CONFIG);
        topic = config.getOrDefault(METRIC_KAFKA_TOPIC, "__normalizer_metrics");
        config
                .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
                .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "rb.ks.serializers.JsonSerializer");
        kafkaProducer = new KafkaProducer<>(config.getProperties());
    }

    @Override
    public void updateMetric(String metricName, Object metricValue) {
        Map<String, Object> metric = new HashMap<>();
        metric.put("timestamp", System.currentTimeMillis() / 1000L);
        metric.put("monitor", metricName);
        metric.put("value", metricValue);
        metric.put("app_id", appId);

        if(metricValue != null)
            kafkaProducer.send(new ProducerRecord<>(topic, appId, metric));
    }

    @Override
    public void close() {
        kafkaProducer.flush();
        kafkaProducer.close();
    }

    @Override
    public String name() {
        return "kafka";
    }
}
