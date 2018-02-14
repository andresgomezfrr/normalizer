package rb.ks.funcs;

import rb.ks.metrics.MetricsManager;

import java.util.Map;

/**
 * Generic interface for definition of functions
 * @param <R> Return type parameter in process method
 */
public interface Function<R> {

    /**
     * Initialize function
     * @param properties Properties for function
     * @param metricsManager MetricsManager object for function
     */
    void init(Map<String, Object> properties, MetricsManager metricsManager);

    /**
     * Prepare function
     * @param properties Properties for function
     * @param metricsManager MetricsManager object for function
     */
    void prepare(Map<String, Object> properties, MetricsManager metricsManager);

    /**
     * Main logic of function
     * @param key The key of Kafka message
     * @param value The value of Kafka message
     * @return Returned value
     */
    R process(String key, Map<String, Object> value);

    /**
     * Stop function
     */
    void stop();
}
