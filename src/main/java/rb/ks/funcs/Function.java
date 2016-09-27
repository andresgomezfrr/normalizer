package rb.ks.funcs;

import rb.ks.metrics.MetricsManager;

import java.util.Map;

public interface Function<R> {
    void init(Map<String, Object> properties, MetricsManager metricsManager);
    void prepare(Map<String, Object> properties, MetricsManager metricsManager);
    R process(String key, Map<String, Object> value);
    void stop();
}
