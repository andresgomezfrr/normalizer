package rb.ks.funcs;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rb.ks.metrics.MetricsManager;

import java.util.Map;

/**
 * This class is used implement mapper process where you can transform one message into other
 */
public abstract class MapperFunction implements Function<KeyValue<String, Map<String, Object>>>,
        KeyValueMapper<String, Map<String, Object>, KeyValue<String, Map<String, Object>>> {
    private final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Initialize mapper function
     * @param properties Properties for mapper function
     * @param metricsManager MetricsManager object for mapper function
     */
    @Override
    public void init(Map<String, Object> properties, MetricsManager metricsManager) {
        prepare(properties, metricsManager);
        log.info("   with {}", toString());
    }

    /**
     * Apply mapper function to Key-Value Kafka message
     * @param key The key of Kafka message
     * @param value The value of Kafka message
     * @return A Key-Value object after apply mapper function
     */
    @Override
    public KeyValue<String, Map<String, Object>> apply(String key, Map<String, Object> value) {
        return process(key, value);
    }
}
