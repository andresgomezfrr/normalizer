package io.wizzie.ks.normalizer.funcs.impl;

import io.wizzie.ks.normalizer.funcs.MapperFunction;
import io.wizzie.metrics.MetricsManager;
import org.apache.kafka.streams.KeyValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SimpleMapper extends MapperFunction {
    List<MapperModel> mappers;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        List<Map<String, Object>> maps = (List<Map<String, Object>>) properties.get("maps");
        mappers = maps.stream()
                .map(map -> new MapperModel((List<String>) map.get("dimPath"), (String) map.get("as")))
                .collect(Collectors.toList());
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {
        Map<String, Object> newEvent = new HashMap<>();

        if (value != null && mappers != null) {
            mappers.forEach(mapper -> {
                Integer depth = mapper.dimPath.size() - 1;

                Map<String, Object> levelPath = new HashMap<>(value);
                for (Integer level = 0; level < depth; level++) {
                    if (levelPath != null) {
                        levelPath = (Map<String, Object>) levelPath.get(mapper.dimPath.get(level));
                    }
                }

                if (levelPath != null) {
                    Object newValue = levelPath.get(mapper.dimPath.get(depth));
                    if (newValue != null) newEvent.put(mapper.as, newValue);
                }
            });

            return new KeyValue<>(key, newEvent);
        } else {
            return new KeyValue<>(key, null);
        }
    }

    @Override
    public void stop() {
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        mappers.forEach(mapper -> builder.append(mapper.toString()));
        builder.append("]");

        return builder.toString();
    }


    public class MapperModel {
        List<String> dimPath;
        String as;

        MapperModel(List<String> dimPath,
                    String as) {
            this.dimPath = dimPath;

            if (as != null) {
                this.as = as;
            } else if (dimPath != null) {
                this.as = dimPath.get(dimPath.size() - 1);
            }
        }

        public List<String> getDimPath() {
            return dimPath;
        }

        public String getAs() {
            return as;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(" {")
                    .append("dimPath: ").append(dimPath).append(", ")
                    .append("as: ").append(as)
                    .append("} ");

            return builder.toString();
        }
    }
}
