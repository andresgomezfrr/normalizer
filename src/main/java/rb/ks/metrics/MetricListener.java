package rb.ks.metrics;

import rb.ks.builder.config.Config;

interface MetricListener {
    void init(Config config);
    void updateMetric(String metricName, Object metricValue);
    void close();
    String name();
}
