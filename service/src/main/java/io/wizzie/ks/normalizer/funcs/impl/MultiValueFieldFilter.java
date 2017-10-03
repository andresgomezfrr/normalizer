package io.wizzie.ks.normalizer.funcs.impl;

import io.wizzie.ks.normalizer.funcs.FilterFunc;
import io.wizzie.ks.normalizer.metrics.MetricsManager;

import java.util.List;
import java.util.Map;

import static io.wizzie.ks.normalizer.utils.Constants.__KEY;

public class MultiValueFieldFilter extends FilterFunc {
    String dimension;
    List<Object> dimensionValues;
    Boolean isDimensionKey = false;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        dimension = (String) properties.get("dimension");
        dimensionValues = (List<Object>) properties.get("values");

        if (dimension == null || dimension.equals(__KEY)) isDimensionKey = true;

    }

    @Override
    public Boolean process(String key, Map<String, Object> value) {
        if (value != null) {
            if (isDimensionKey) {
                return dimensionValues.contains(key);
            } else {
                Object currentValue = value.get(dimension);
                return currentValue != null && dimensionValues.contains(currentValue);
            }
        } else {
            return false;
        }
    }

    @Override
    public void stop() {

    }
}
