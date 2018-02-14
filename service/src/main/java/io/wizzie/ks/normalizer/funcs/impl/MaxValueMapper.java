package io.wizzie.ks.normalizer.funcs.impl;

import io.wizzie.ks.normalizer.funcs.MapperFunction;
import io.wizzie.ks.normalizer.metrics.MetricsManager;
import org.apache.kafka.streams.KeyValue;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class MaxValueMapper extends MapperFunction {

    String dimension;
    String newDimension;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        dimension = (String) checkNotNull(properties.get("dimension"), "dimension cannot be null");
        newDimension = (String) properties.getOrDefault("max_dimension_name", "max_value");
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {

        if(value != null) {
            List arrayValue = (List<Number>) value.get(dimension);

            if(arrayValue != null)
                value.put(newDimension, Collections.max(arrayValue));
        }

        return new KeyValue<>(key, value);
    }

    @Override
    public void stop() {

    }
}
