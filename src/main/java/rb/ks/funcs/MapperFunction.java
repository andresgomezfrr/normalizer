package rb.ks.funcs;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.Map;


public abstract class MapperFunction implements Function<KeyValue<String, Map<String, Object>>>,
        KeyValueMapper<String, Map<String, Object>, KeyValue<String, Map<String, Object>>> {

    @Override
    public void init(Map<String, Object> properties) {
        prepare(properties);
    }

    @Override
    public KeyValue<String, Map<String, Object>> apply(String key, Map<String, Object> value) {
        return process(key, value);
    }
}
