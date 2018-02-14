package zz.ks.funcs.impl;

import zz.ks.funcs.FilterFunc;
import zz.ks.metrics.MetricsManager;
import zz.ks.utils.Constants;

import java.util.Map;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class StartWithFilter extends FilterFunc {

    String startWithValue;
    String dimension;
    Boolean isDimensionKey = false;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        startWithValue = checkNotNull((String) properties.get("start_with"), "start_with cannot be null");
        dimension = checkNotNull((String) properties.get("dimension"), "dimension cannot be null");

        if(dimension.equals(Constants.__KEY)) isDimensionKey = true;
    }

    @Override
    public Boolean process(String key, Map<String, Object> value) {
        if(isDimensionKey && key != null)
            return key.startsWith(startWithValue);
        else if(!isDimensionKey) {
            Object currentValue = value.get(dimension);
            return currentValue != null && currentValue.toString().startsWith(startWithValue);
        }

        return false;
    }

    @Override
    public void stop() {

    }
}
