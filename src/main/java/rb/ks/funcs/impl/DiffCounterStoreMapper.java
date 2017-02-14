package rb.ks.funcs.impl;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import rb.ks.funcs.MapperStoreFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static rb.ks.utils.Constants.*;
import static rb.ks.utils.ConversionUtils.toLong;

public class DiffCounterStoreMapper extends MapperStoreFunction {
    List<String> counterFields;
    KeyValueStore<String, Map<String, Long>> storeCounter;
    Boolean sendIfZero;
    String timestampField;
    List<String> keys;
    Boolean firstTimeView;

    @Override
    public void prepare(Map<String, Object> properties) {
        counterFields = (List<String>) properties.get("counters");
        storeCounter = getStore("counter-store");
        sendIfZero = (Boolean) properties.get("sendIfZero");
        timestampField = String.valueOf(properties.get("timestamp"));
        keys = (List<String>) properties.get("keys");
        firstTimeView = (Boolean) properties.get("firsttimeview");

        if (firstTimeView == null) firstTimeView = true;

        if (sendIfZero == null) sendIfZero = true;

        timestampField = timestampField != null ? timestampField : "timestamp";
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {

        String definedKey = "";

        if (keys != null && !keys.isEmpty()) {

            definedKey = "";

            for (String keyElement : keys) {

                if(keyElement.equals(__KEY))
                    definedKey += key;
                else if (value.containsKey(keyElement))
                    definedKey += value.get(keyElement).toString();

            }

            if (definedKey.isEmpty())
                definedKey = key;
        }


        Map<String, Long> newCounters = new HashMap<>();
        Map<String, Long> newTimestamp = new HashMap<>();

        for (String counterField : counterFields) {
            Long counter = toLong(value.remove(counterField));
            if (counter != null) newCounters.put(counterField, counter);
        }

        Long timestampValue = toLong(value.get(timestampField));

        if (timestampValue != null) {
            newTimestamp.put(timestampField, timestampValue);
        } else {
            timestampValue = System.currentTimeMillis() / 1000;
            value.put(timestampField, timestampValue);
            newTimestamp.put(timestampField, timestampValue);
        }

        Map<String, Long> counters = storeCounter.get(definedKey);

        if (counters != null) {

            for (Map.Entry<String, Long> counter : newCounters.entrySet()) {
                Long lastValue = toLong(counters.get(counter.getKey()));
                if (lastValue != null) {
                    Long diff = counter.getValue() - lastValue;
                    if (diff != 0 || sendIfZero) value.put(counter.getKey(), diff);
                }
            }

            Long lastTimestamp = toLong(counters.get(this.timestampField));

            if (lastTimestamp != null) {
                value.put("last_timestamp", lastTimestamp);
            }

            counters.putAll(newCounters);
            counters.putAll(newTimestamp);

        } else {

            if(!firstTimeView) value = null;

            counters = newCounters;
            counters.putAll(newTimestamp);
        }

        storeCounter.put(definedKey, counters);

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
                .append("timestamp: ").append(timestampField).append(", ")
                .append("keys: ").append(keys).append(", ")
                .append("firsttimeview: ").append(firstTimeView)
                .append("} ");

        return builder.toString();
    }

    @Override
    public KeyValue<String, Map<String, Object>> window(long timestamp) {
        //Nothing to do.
        return null;
    }
}
