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
    String key = "__KEY";

    @Override
    public void prepare(Map<String, Object> properties) {
        counterFields = (List<String>) properties.get("counters");
        storeCounter = getStore("counter-store");
        sendIfZero = (Boolean) properties.get("sendIfZero");
        timestamp = String.valueOf(properties.get("timestamp"));
        List<String> keys = (List<String>) properties.get("key");


        if(keys != null) key = keys.stream().reduce("", String::concat);

        if (sendIfZero == null) sendIfZero = true;

        timestamp = timestamp != null ? timestamp : "timestamp";
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {
        Map<String, Long> newCounters = new HashMap<>();
        Map<String, Long> newTimestamp = new HashMap<>();

        for (String counterField : counterFields) {
            Long counter = toLong(value.remove(counterField));
            if (counter != null) newCounters.put(counterField, counter);
        }

        Long timestamp = toLong(value.remove(this.timestamp));

        if(timestamp != null) {
            newTimestamp.put(this.timestamp, timestamp);
        } else {
            timestamp = System.currentTimeMillis()/1000;
            newTimestamp.put(this.timestamp, timestamp);
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

            Long lastTimestamp = toLong(counters.get(this.timestamp));

            if(lastTimestamp != null) {
                value.put("last_timestamp", lastTimestamp);
                value.put(this.timestamp, timestamp);
            }

            counters.putAll(newCounters);
            counters.putAll(newTimestamp);

        } else {
            value.put(this.timestamp, timestamp);
            counters = newCounters;
            counters.putAll(newTimestamp);
        }

        if(this.key.equals("__KEY"))
            storeCounter.put(key, counters);
        else
            storeCounter.put(this.key, counters);

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
                .append("timestamp: ").append(timestamp)
                .append("} ");

        return builder.toString();
    }
}
