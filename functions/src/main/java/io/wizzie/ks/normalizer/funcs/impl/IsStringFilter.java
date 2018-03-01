package io.wizzie.ks.normalizer.funcs.impl;

import io.wizzie.ks.normalizer.funcs.FilterFunc;
import io.wizzie.metrics.MetricsManager;

import java.util.Map;

import static io.wizzie.ks.normalizer.base.utils.Constants.__KEY;

public class IsStringFilter extends FilterFunc {
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
            return currentValue instanceof String;

        } else {
            return false;
        }
    }

    @Override
    public void stop() {

    }
}
