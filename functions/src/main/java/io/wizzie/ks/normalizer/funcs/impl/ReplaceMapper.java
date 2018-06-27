package io.wizzie.ks.normalizer.funcs.impl;

import io.wizzie.ks.normalizer.funcs.MapperFunction;
import io.wizzie.metrics.MetricsManager;
import org.apache.kafka.streams.KeyValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class ReplaceMapper extends MapperFunction {

    List<Map<String, Object>> replacements;
    Map<String, Map<Object, Object>> replacementsMap = new HashMap<>();


    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        replacements = checkNotNull((List<Map<String, Object>>) properties.get("replacements"), "replacements cannot be null");

        for(Map<String, Object> entry : replacements){
            String dimension = (String)entry.get("dimension");
            List<Map<String, Object>> replaceValues = (List<Map<String, Object>>)entry.get("replacements");
            Map<Object,Object> valuesToReplace = new HashMap<>();
            for(Map<String, Object> replaceEntry : replaceValues){
                valuesToReplace.put(replaceEntry.get("from"),replaceEntry.get("to"));
            }
            replacementsMap.put(dimension, valuesToReplace);
        }
        for(Map.Entry replacementsMapEntry : replacementsMap.entrySet()){
            checkNotNull(replacementsMapEntry.getKey());
            checkNotNull(replacementsMapEntry.getValue());
        }
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {

        if (value != null) {
            for(Map.Entry<String, Map<Object, Object>> replacement : replacementsMap.entrySet()) {
                Object valueToReplace = value.get(replacement.getKey());
                if (valueToReplace != null) {
                    for(Map.Entry newValue : replacement.getValue().entrySet()) {
                        if (newValue.getKey().equals(valueToReplace))
                            value.put(replacement.getKey(), newValue.getValue());
                    }
                }
            }
            return new KeyValue<>(key, value);
        } else {
            return new KeyValue<>(key, null);
        }
    }

    @Override
    public void stop() {

    }
}
