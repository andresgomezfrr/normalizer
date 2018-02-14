package io.wizzie.ks.normalizer.funcs.impl;

import io.wizzie.ks.normalizer.funcs.MapperFunction;
import io.wizzie.ks.normalizer.metrics.MetricsManager;
import org.apache.kafka.streams.KeyValue;

import java.util.Map;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class StringReplaceMapper extends MapperFunction {

    String dimension;
    String targetString;
    String replacementString;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        dimension = (String) checkNotNull(properties.get("dimension"), "dimension cannot be null");
        targetString = (String) checkNotNull(properties.get("target_string"), "target_string cannot be null");
        replacementString = (String) checkNotNull(properties.get("replacement_string"), "replacement_string cannot be null");
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {

        if(value != null) {

            String content = (String) value.get(dimension);

            if(content != null) {
                value.put(dimension, content.replaceAll(targetString, replacementString));
            }

        }

        return new KeyValue<>(key, value);
    }

    @Override
    public void stop() {

    }
}
