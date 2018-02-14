package rb.ks.builder.config;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class Config {
    Map<String, Object> config;

    public Config(String configPath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        config = objectMapper.readValue(new File(configPath), Map.class);
    }

    public <T> T get(String property) {
        T ret = null;

        if (config != null) {
            ret = (T) config.get(property);
        }

        return ret;
    }

    public static class ConfigProperties {
        public static final String BOOTSTRAPER_CLASSNAME = "bootstraper.classname";
    }
}
