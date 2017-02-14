package rb.ks.funcs;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rb.ks.utils.ConversionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static rb.ks.utils.Constants.*;
public abstract class MapperStoreFunction implements Function<KeyValue<String, Map<String, Object>>>,
        Transformer<String, Map<String, Object>, KeyValue<String, Map<String, Object>>> {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private Map<String, Object> properties;
    private Map<String, KeyValueStore> stores = new HashMap<>();
    private List<String> availableStores;
    private String appId;
    private Long windownTimeMs;

    @Override
    public void init(Map<String, Object> properties) {
        this.availableStores = (List<String>) properties.get(__STORES);
        this.appId = (String) properties.get(__APP_ID);
        this.windownTimeMs = ConversionUtils.toLong(properties.get(__WINDOW_TIME_MS));
        this.properties = properties;
    }

    @Override
    public void init(ProcessorContext context) {
        if(windownTimeMs != null) context.schedule(windownTimeMs);
        availableStores.forEach((storeName) ->
                stores.put(storeName, (KeyValueStore) context.getStateStore(String.format("%s_%s", appId, storeName)))
        );
        prepare(properties);
        log.info("   with {}", toString());
    }

    @Override
    public KeyValue<String, Map<String, Object>> transform(String key, Map<String, Object> value) {
        return process(key, value);
    }

    @Override
    public KeyValue<String, Map<String, Object>> punctuate(long timestamp) {
        return window(timestamp);
    }

    @Override
    public void close() {
        stores.values().forEach(StateStore::close);
        stop();
    }

    public List<String> getAvailableStores(){
        return availableStores;
    }

    public <V> KeyValueStore<String, V> getStore(String storeName){
        return (KeyValueStore<String, V>) stores.get(storeName);
    }

    public abstract KeyValue<String, Map<String, Object>> window(long timestamp);

}
