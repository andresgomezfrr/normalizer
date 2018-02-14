package rb.ks.funcs.impl;

import org.apache.kafka.streams.KeyValue;
import rb.ks.funcs.FlatMapperFunction;
import rb.ks.metrics.MetricsManager;

import java.util.*;
import java.util.stream.Collectors;

public class ArrayFlattenMapper extends FlatMapperFunction {

    String flatDimension;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        flatDimension = (String) properties.get("flat_dimension");
    }

    @Override
    public Iterable<KeyValue<String, Map<String, Object>>> process(String key, Map<String, Object> value) {
        List<KeyValue<String, Map<String, Object>>> results = Collections.singletonList(new KeyValue<>(key, value));

        if (flatDimension != null) {

            if (value.containsKey(flatDimension)) {
                List<String> array = (List<String>) value.remove(flatDimension);

                if(array != null) {
                    results = array.stream().map(val -> {

                        Map<String, Object> newValue = new HashMap<>();
                        newValue.putAll(value);
                        newValue.put(flatDimension, val);

                        KeyValue<String, Map<String, Object>> kv = new KeyValue<>(key, newValue);

                        return kv;

                    }).collect(Collectors.toList());
                }

            }

        }

        return results;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(" {")
                .append("flat_dimension: ").append(flatDimension).append("} ");

        return builder.toString();
    }

    @Override
    public void stop() {

    }
}
