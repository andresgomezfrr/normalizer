package rb.ks.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonSerializer implements Serializer<Map<String, Object>> {
    ObjectMapper mapper = new ObjectMapper();

    public void configure(Map<String, ?> configs, boolean isKey) {
        // Nothing to do
    }

    public byte[] serialize(String topic, Map<String, Object> data) {
        byte[] raw = null;
        try {
            raw = mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return raw;
    }

    public void close() {
        // Nothing to do
    }
}
