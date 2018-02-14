package rb.ks.funcs;

import org.apache.kafka.streams.kstream.Predicate;

import java.util.Map;

public abstract class FilterFunc implements Function<Boolean>, Predicate<String, Map<String, Object>> {
    @Override
    public void init(Map<String, Object> properties) {
        prepare(properties);
    }

    @Override
    public boolean test(String key, Map<String, Object> value) {
        return process(key, value);
    }
}
