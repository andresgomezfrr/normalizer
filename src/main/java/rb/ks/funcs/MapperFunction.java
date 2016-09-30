package rb.ks.funcs;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rb.ks.metrics.MetricsManager;

import java.util.Map;


public abstract class MapperFunction implements Function<KeyValue<String, Map<String, Object>>>,
        KeyValueMapper<String, Map<String, Object>, KeyValue<String, Map<String, Object>>> {
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public void init(Map<String, Object> properties, MetricsManager metricsManager) {
        prepare(properties, metricsManager);
        log.info("   with {}", toString());
    }

    @Override
    public KeyValue<String, Map<String, Object>> apply(String key, Map<String, Object> value) {
        return process(key, value);
    }
}
