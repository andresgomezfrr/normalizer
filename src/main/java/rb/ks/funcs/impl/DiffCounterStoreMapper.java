package rb.ks.funcs.impl;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import rb.ks.funcs.MapperStoreFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static rb.ks.utils.ConversionUtils.toLong;

public class DiffCounterStoreMapper extends MapperStoreFunction {
    List<String> counterFields;
    KeyValueStore<String, Map<String, Long>> storeCounter;
    Boolean sendIfZero;
    String timestamp;

    @Override
    public void prepare(Map<String, Object> properties) {
        counterFields = (List<String>) properties.get("counters");
        storeCounter = getStore("counter-store");
        sendIfZero = (Boolean) properties.get("sendIfZero");
        timestamp = String.valueOf(properties.get("timestamp"));

        if (sendIfZero == null) sendIfZero = true;

        timestamp = timestamp != null ? timestamp : "timestamp";
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {
        Map<String, Long> newCounters = new HashMap<>();

        for (String counterField : counterFields) {
            Long counter = toLong(value.remove(counterField));
            if (counter != null) newCounters.put(counterField, counter);
        }

        Long timestampAfterValue = toLong(value.remove(timestamp));

        if(timestampAfterValue != null) {
            newCounters.put(timestamp, timestampAfterValue);
        } else {
            newCounters.put(timestamp, System.currentTimeMillis()/1000);
        }

        Map<String, Long> counters = storeCounter.get(key);

        if (counters != null) {

            for (Map.Entry<String, Long> counter : newCounters.entrySet()) {
                Long lastValue = toLong(counters.get(counter.getKey()));
                if (lastValue != null) {
                    Long diff = counter.getValue() - lastValue;
                    if (diff != 0 || sendIfZero) value.put(counter.getKey(), diff);
                }
            }

            Long timestampBeforeValue = counters.get(timestamp);

            if(timestampBeforeValue != null) {
                value.put("timestamp_before", timestampBeforeValue);
                value.put("timestamp_after", timestampAfterValue);
            } else {
                value.put("timestamp_before", timestampAfterValue);
                value.put("timestamp_after", System.currentTimeMillis()/1000);
            }

            counters.putAll(newCounters);

        } else {
            counters = newCounters;
        }

        storeCounter.put(key, counters);

        return new KeyValue<>(key, value);
    }

    @Override
    public void stop() {
        if (counterFields != null) counterFields.clear();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(" {")
                .append("counters: ").append(counterFields).append(", ")
                .append("sendIfZero: ").append(sendIfZero).append(", ")
                .append("stores: ").append(storeCounter.name()).append(", ")
                .append("tiemstamp: ").append(timestamp)
                .append("} ");

        return builder.toString();
    }
}
