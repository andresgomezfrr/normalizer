package io.wizzie.ks.normalizer.funcs.impl;

import io.wizzie.ks.normalizer.funcs.FilterFunc;
import io.wizzie.ks.normalizer.metrics.MetricsManager;

import java.util.Map;

import static io.wizzie.ks.normalizer.utils.Constants.__KEY;

public class FieldFilter extends FilterFunc {
    String dimension;
    Object dimensionValue;
    Boolean isDimensionKey = false;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        dimension = (String) properties.get("dimension");
        dimensionValue = properties.get("value");

        if (dimension == null || dimension.equals(__KEY)) isDimensionKey = true;

    }

    @Override
    public Boolean process(String key, Map<String, Object> value) {
        if (isDimensionKey) {
            return key.equals(dimensionValue);
        } else {
            Object currentValue = value.get(dimension);
            return currentValue != null && currentValue.equals(dimensionValue);
        }
    }

    @Override
    public void stop() {

    }
}
