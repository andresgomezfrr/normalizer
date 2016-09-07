package rb.ks.funcs;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.Map;

public abstract class FlatMapperFunction implements Function<Iterable<KeyValue<String, Map<String, Object>>>>,
        KeyValueMapper<String, Map<String, Object>, Iterable<KeyValue<String, Map<String, Object>>>> {

    @Override
    public void init(Map<String, Object> properties) {
        prepare(properties);
    }

    @Override
    public Iterable<KeyValue<String, Map<String, Object>>> apply(String key, Map<String, Object> value) {
        return process(key, value);
    }
}
