package rb.ks.funcs;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rb.ks.metrics.MetricsManager;

import java.util.Map;

/**
 * This class is used to implement flat-mapper process where you can generate zero or more messages from one message
 */
public abstract class FlatMapperFunction implements Function<Iterable<KeyValue<String, Map<String, Object>>>>,
        KeyValueMapper<String, Map<String, Object>, Iterable<KeyValue<String, Map<String, Object>>>> {
    private final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Initialize flat mapper function
     * @param properties Properties for flat mapper function
     * @param metricsManager MetricsManager object for flat mapper function
     */
    @Override
    public void init(Map<String, Object> properties, MetricsManager metricsManager) {
        prepare(properties, metricsManager);
        log.info("   with {}", toString());
    }

    /**
     * Apply a flat mapper function to Key-Value Kafka message
     * @param key The key of Kafka message
     * @param value The value of Kafka message
     * @return A Iterable object with Kafka messages after apply flat mapper
     */
    @Override
    public Iterable<KeyValue<String, Map<String, Object>>> apply(String key, Map<String, Object> value) {
        return process(key, value);
    }
}
