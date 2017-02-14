package rb.ks.funcs.impl;

import rb.ks.funcs.FilterFunc;
import rb.ks.utils.Constants;

import java.util.Map;

public class FieldFilter extends FilterFunc {
    private String dimension;
    private Object dimensionValue;
    private Boolean isDimensionKey;

    @Override
    public void prepare(Map<String, Object> properties) {
        dimension = (String) properties.get("dimension");
        dimensionValue = properties.get("value");

        if (dimension == null || dimension.equals(Constants.__KEY)) isDimensionKey = true;

    }

    @Override
    public Boolean process(String key, Map<String, Object> value) {
        if (isDimensionKey) {
            return key.equals(dimensionValue);
        } else {
            Object currentValue = value.get(dimension);
            return currentValue.equals(dimensionValue);
        }
    }

    @Override
    public void stop() {

    }
}
