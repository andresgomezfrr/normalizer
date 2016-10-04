package rb.ks.builder.bootstrap;

import rb.ks.builder.Builder;
import rb.ks.builder.config.Config;
import rb.ks.metrics.MetricsManager;

public interface Bootstraper {
    void init(Builder builder, Config config, MetricsManager metricsManager) throws Exception;
    void close();
}
