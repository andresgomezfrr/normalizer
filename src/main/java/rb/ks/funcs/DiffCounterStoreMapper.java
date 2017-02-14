package rb.ks.funcs;

import org.apache.kafka.streams.KeyValue;
import org.jboss.netty.util.internal.ConversionUtil;
import rb.ks.utils.ConversionUtils;

import java.util.List;
import java.util.Map;

public class DiffCounterStoreMapper extends MapperStoreFunction {
    List<String> counters;

    @Override
    public void prepare(Map<String, Object> properties) {
        counters = (List<String>) properties.get("counters");
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {
        for(String counterField : counters){
            Long counter = ConversionUtils.toLong(value.remove(counterField);
        }

        return new KeyValue<>(key, value);
    }

    @Override
    public void stop() {

    }
}
