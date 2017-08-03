package io.wizzie.ks.normalizer.metrics;

import io.wizzie.bootstrapper.builder.Config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockMetricListener implements MetricListener {

    List<Map<String, Object>> content = new ArrayList<>();

    @Override
    public void init(Config config) {
    }

    @Override
    public void updateMetric(String metricName, Object metricValue) {
        Map<String, Object> metric = new HashMap<>();
        metric.put("timestamp", System.currentTimeMillis() / 1000L);
        metric.put("monitor", metricName);
        metric.put("value", metricValue);

        if (metricValue != null)
            content.add(metric);
    }

    @Override
    public void close() {
        //Nothing to do
    }

    @Override
    public String name() {
        return "mock";
    }
}
