package rb.ks.funcs;

import org.apache.kafka.streams.kstream.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rb.ks.metrics.MetricsManager;
import rb.ks.utils.Constants;

import java.util.Map;

public abstract class FilterFunc implements Function<Boolean>, Predicate<String, Map<String, Object>> {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private Boolean __MATCH = true;

    @Override
    public void init(Map<String, Object> properties, MetricsManager metricsManager) {
        __MATCH = properties.containsKey(Constants.__MATCH) ? (Boolean) properties.get(Constants.__MATCH) : true;
        prepare(properties, metricsManager);
        log.info("   with {}", toString());
    }

    @Override
    public boolean test(String key, Map<String, Object> value) {
        return process(key, value);
    }

    public Boolean match() {
        return __MATCH;
    }

}
