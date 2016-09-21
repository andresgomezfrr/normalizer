package rb.ks.funcs.impl;

import rb.ks.funcs.FilterFunc;

import java.util.List;
import java.util.Map;

public class ContainsDimensionFilter extends FilterFunc {
    List<String> dimensions;

    @Override
    public void prepare(Map<String, Object> properties) {
        dimensions = (List<String>) properties.get("dimensions");
    }

    @Override
    public Boolean process(String key, Map<String, Object> value) {
        if(value == null) return false;
        else return dimensions.stream().map(value::containsKey).reduce((x,y) -> x ? y : x).orElseGet(() -> Boolean.FALSE);
    }

    @Override
    public void stop() {

    }
}
