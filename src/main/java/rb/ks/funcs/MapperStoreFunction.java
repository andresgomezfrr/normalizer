package rb.ks.funcs;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class MapperStoreFunction implements Function<KeyValue<String, Map<String, Object>>>,
        Transformer<String, Map<String, Object>, KeyValue<String, Map<String, Object>>> {

    private Map<String, KeyValueStore> stores = new HashMap<>();
    private List<String> availableStores;

    @Override
    public void init(Map<String, Object> properties) {
        this.availableStores = (List<String>) properties.get("__STORES");
        prepare(properties);
    }

    @Override
    public void init(ProcessorContext context) {
        availableStores.forEach((storeName) -> stores.put(storeName, (KeyValueStore) context.getStateStore(storeName)));
    }

    @Override
    public KeyValue<String, Map<String, Object>> transform(String key, Map<String, Object> value) {
        return process(key, value);
    }

    @Override
    public KeyValue<String, Map<String, Object>> punctuate(long timestamp) {
        // Currently the MapperStore not support windows.
        return null;
    }

    @Override
    public void close() {
        stores.values().forEach(StateStore::close);
        stop();
    }

    public <K, V> KeyValue<K, V> getStore(String storeName){
        return (KeyValue<K, V>) stores.get(storeName);
    }


}
