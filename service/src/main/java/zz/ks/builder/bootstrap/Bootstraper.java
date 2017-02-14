package zz.ks.builder.bootstrap;

import zz.ks.builder.Builder;
import zz.ks.builder.config.Config;
import zz.ks.metrics.MetricsManager;

/**
 * A simple interface for self-starting process definition
 */
public interface Bootstraper {

    /**
     * Allow initialize the boot process
     *
     * @param builder A builder object
     * @param config Bootstrap configuration
     * @param metricsManager A MetricsManager object
     * @throws Exception Throws IOException and PlanBuilderException
     */
    void init(Builder builder, Config config, MetricsManager metricsManager) throws Exception;

    /**
     * End bootstrap process
     */
    void close();
}
