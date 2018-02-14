package rb.ks.funcs;

import org.apache.kafka.streams.kstream.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rb.ks.metrics.MetricsManager;
import rb.ks.utils.Constants;

import java.util.Map;

/**
 * Abstract class filter function
 */
public abstract class FilterFunc implements Function<Boolean>, Predicate<String, Map<String, Object>> {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private Boolean __MATCH = true;

    /**
     * Initalize filter function
     * @param properties Properties for filter function
     * @param metricsManager MetricsManager object for filter function
     */
    @Override
    public void init(Map<String, Object> properties, MetricsManager metricsManager) {
        __MATCH = properties.containsKey(Constants.__MATCH) ? (Boolean) properties.get(Constants.__MATCH) : true;
        prepare(properties, metricsManager);
        log.info("   with {}", toString());
    }

    /**
     * Filter a Key-Value Kafka message
     * @param key The key of Kafka message
     * @param value The value of kafka message
     * @return True if filter match else return false.
     */
    @Override
    public boolean test(String key, Map<String, Object> value) {
        return process(key, value);
    }

    /**
     * Check __MATCH flag
     * @return True if __MATCH is true or false else.
     */
    public Boolean match() {
        return __MATCH;
    }

}
