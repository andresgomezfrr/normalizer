package rb.ks.builder.config;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Config {
    Map<String, Object> config;

    public Config() {
        config = new HashMap<>();
    }

    public Config(String configPath) throws IOException {
        init(configPath);
    }

    public Config(Map<String, Object> properties) {
        init(properties);
    }

    private void init(String configPath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        init(objectMapper.readValue(new File(configPath), Map.class));
    }

    private void init(Map<String, Object> properties) {
        config = properties;
    }

    public <T> T get(String property) {
        T ret = null;

        if (config != null) {
            ret = (T) config.get(property);
        }

        return ret;
    }

    public Config put(String property, Object value) {

        config.put(property, value);

        return this;
    }

    public static class ConfigProperties {
        public static final String BOOTSTRAPER_CLASSNAME = "bootstraper.classname";
    }
}
