package io.wizzie.ks.metrics;

import io.wizzie.ks.builder.config.Config;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConsoleMetricListenerUnitTest {

    @Test
    public void metricManagerShouldHaveAConsoleMetricListener() {
        Config config = new Config();

        config
                .put("metric.enable", true)
                .put("metric.interval", 2000)
                .put("application.id", "testing-metric-manager")
                .put("num.stream.threads", 1)
                .put("metric.listeners", Collections.singletonList("io.wizzie.ks.metrics.ConsoleMetricListener"));

        MetricsManager metricsManager = new MetricsManager(config);

        assertFalse(metricsManager.listeners.isEmpty());

        assertTrue(metricsManager.listeners.get(0) instanceof ConsoleMetricListener);

    }
}
