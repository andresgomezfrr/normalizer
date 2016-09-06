package zz.ks.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class JsonDeserializer implements Deserializer<Map<String, Object>> {
    ObjectMapper mapper = new ObjectMapper();

    public void configure(Map configs, boolean isKey) {

    }

    public Map<String, Object> deserialize(String topic, byte[] data) {
        Map<String, Object> json = null;
        if(data != null) {
            try {
                json = mapper.readValue(data, Map.class);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return json;
    }

    public void close() {

    }
}
