package rb.ks.funcs;

import org.apache.kafka.streams.KeyValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleMapper extends MapperFunction {
    List<MapperModel> mappers = new ArrayList<>();

    @Override
    public void prepare(Map<String, Object> properties) {
        List<Map<String, Object>> maps = (List<Map<String, Object>>) properties.get("maps");
        maps.forEach(map -> mappers.add(new MapperModel((List<String>) map.get("dimPath"), (String) map.get("as"))));
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {
        Map<String, Object> newEvent = new HashMap<>();
        if (value != null) {
            for (MapperModel mapper : mappers) {
                Integer depth = mapper.dimPath.size() - 1;

                Map<String, Object> levelPath = new HashMap<>(value);
                for (Integer level = 0; level < depth; level++) {
                    if (levelPath != null) {
                        levelPath = (Map<String, Object>) levelPath.get(mapper.dimPath.get(level));
                    }
                }

                if (levelPath != null) {
                    Object newValue = levelPath.get(mapper.dimPath.get(depth));
                    if (newValue != null) newEvent.put(mapper.as, newValue);
                }
            }

            return new KeyValue<>(key, newEvent);
        } else {
            return new KeyValue<>(key, null);
        }
    }

    @Override
    public void stop() {
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        mappers.forEach(mapper -> builder.append(mapper.toString()));
        builder.append("]");

        return builder.toString();
    }


    public class MapperModel {
        List<String> dimPath;
        String as;

        MapperModel(List<String> dimPath,
                    String as) {
            this.dimPath = dimPath;

            if (as != null) {
                this.as = as;
            } else if (dimPath != null) {
                this.as = dimPath.get(dimPath.size() - 1);
            }
        }

        public List<String> getDimPath() {
            return dimPath;
        }

        public String getAs() {
            return as;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(" {")
                    .append("dimPath: ").append(dimPath).append(", ")
                    .append("as: ").append(as)
                    .append("} ");

            return builder.toString();
        }
    }
}
