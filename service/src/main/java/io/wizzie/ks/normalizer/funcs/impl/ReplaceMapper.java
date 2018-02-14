package io.wizzie.ks.normalizer.funcs.impl;

import io.wizzie.ks.normalizer.funcs.MapperFunction;
import io.wizzie.ks.normalizer.metrics.MetricsManager;
import org.apache.kafka.streams.KeyValue;

import java.util.Map;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class ReplaceMapper extends MapperFunction {

    String dimension;
    Map<String, String> replacements;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        replacements = checkNotNull((Map<String, String>) properties.get("replacements"), "replacements cannot be null");
        dimension = checkNotNull((String) properties.get("dimension"), "dimension cannot be null");
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {

        String replaceValue = (String) value.get(dimension);

        if(replaceValue != null){
            replaceValue = replaceValue.toLowerCase();

            if(replacements.containsKey(replaceValue))
                value.put(dimension, replacements.get(replaceValue));
        }

        return new KeyValue<>(key, value);
    }

    @Override
    public void stop() {

    }
}
