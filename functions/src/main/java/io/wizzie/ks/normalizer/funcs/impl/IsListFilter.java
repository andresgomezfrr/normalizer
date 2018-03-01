package io.wizzie.ks.normalizer.funcs.impl;

import io.wizzie.ks.normalizer.funcs.FilterFunc;
import io.wizzie.metrics.MetricsManager;

import java.util.List;
import java.util.Map;

public class IsListFilter extends FilterFunc {
    String dimension;
    Boolean isDimensionKey = false;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        dimension = (String) properties.get("dimension");

        if (dimension == null) isDimensionKey = true;
    }

    @Override
    public Boolean process(String key, Map<String, Object> value) {
        if (value != null) {
            Object currentValue = value.get(dimension);
            return currentValue instanceof List;

        } else {
            return false;
        }
    }

    @Override
    public void stop() {

    }
}
