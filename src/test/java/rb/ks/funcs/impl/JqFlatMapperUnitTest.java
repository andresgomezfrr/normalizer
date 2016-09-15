package rb.ks.funcs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KeyValue;
import org.junit.BeforeClass;
import org.junit.Test;
import rb.ks.StreamBuilder;
import rb.ks.exceptions.PlanBuilderException;
import rb.ks.funcs.Function;
import rb.ks.model.PlanModel;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class JqFlatMapperUnitTest {
    static StreamBuilder streamBuilder = new StreamBuilder();
    static JqFlatMapper jqFlatMapper;

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("jq-flat-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
        Map<String, Function> functionsMap = streamBuilder.getFunctions("stream1");
        jqFlatMapper = (JqFlatMapper) functionsMap.get("jqFlatMapper");
    }

    @Test
    public void building() {
        assertEquals("{ids:[.ids|split(\",\")[]|tonumber|.+100],name}", jqFlatMapper.jqQuery);
    }

    @Test
    public void processSingleMessage() {
        String key = "key1";
        Map<String, Object> message = new HashMap<>();
        message.put("ids", "12,15,23");
        message.put("name", "jqTesting");
        message.put("timestamp", 1418785331123L);

        List<KeyValue<String, Map<String, Object>>> result =
                (List<KeyValue<String, Map<String, Object>>>) jqFlatMapper.process(key, message);

        Map<String, Object> expected = new HashMap<>();
        expected.put("name", "jqTesting");
        expected.put("ids", Arrays.asList(112, 115, 123));

        assertEquals(key, result.get(0).key);
        assertEquals(expected, result.get(0).value);
    }

    @Test
    public void processNullMessage() {
        String key = "key1";
        List<KeyValue<String, Map<String, Object>>> result =
                (List<KeyValue<String, Map<String, Object>>>) jqFlatMapper.process(key, null);

        assertTrue(result.isEmpty());
    }
}
