package rb.ks.funcs;

import org.apache.kafka.streams.kstream.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class FilterFunc implements Function<Boolean>, Predicate<String, Map<String, Object>> {
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public void init(Map<String, Object> properties) {
        prepare(properties);
        log.info("   with {}", toString());
    }

    @Override
    public boolean test(String key, Map<String, Object> value) {
        return process(key, value);
    }
}
