package rb.ks.funcs;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static rb.ks.utils.ConversionUtils.*;

public class DiffCounterStoreMapper extends MapperStoreFunction {
    List<String> counterFields;
    KeyValueStore<String, Map<String, Long>> storeCounter;
    Boolean sendIfZero;

    @Override
    public void prepare(Map<String, Object> properties) {
        counterFields = (List<String>) properties.get("counters");
        storeCounter = getStore("counter-store");
        sendIfZero = (Boolean) properties.get("sendIfZero");
        if (sendIfZero == null) sendIfZero = true;
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {
        Map<String, Long> newCounters = new HashMap<>();

        for (String counterField : counterFields) {
            Long counter = toLong(value.remove(counterField));
            if (counter != null) newCounters.put(counterField, counter);
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
}
