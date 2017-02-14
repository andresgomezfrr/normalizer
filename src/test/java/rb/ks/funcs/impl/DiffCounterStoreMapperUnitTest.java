package rb.ks.funcs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KeyValue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import rb.ks.StreamBuilder;
import rb.ks.exceptions.PlanBuilderException;
import rb.ks.funcs.Function;
import rb.ks.funcs.impl.DiffCounterStoreMapper;
import rb.ks.mocks.MockProcessContext;
import rb.ks.model.PlanModel;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DiffCounterStoreMapperUnitTest {
    private static StreamBuilder streamBuilder = new StreamBuilder("app-id-1");
    private static DiffCounterStoreMapper diffCounterStoreMapper;

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("diff-counter-store-mapper-stream.json").getFile());
        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);

        streamBuilder.builder(model);
        Map<String, Function> functionsMap = streamBuilder.getFunctions("stream1");
        diffCounterStoreMapper = (DiffCounterStoreMapper) functionsMap.get("diffCounterMapper");
        diffCounterStoreMapper.init(new MockProcessContext());
    }

    @Test
    public void building(){
        assertEquals(true, diffCounterStoreMapper.sendIfZero);
        assertEquals(Collections.singletonList("X"), diffCounterStoreMapper.counterFields);
        assertNotNull(diffCounterStoreMapper.storeCounter);
    }

    @Test
    public void processSimpleMessage() {
        String key1 = randomKey();

        Map<String, Object> message1 = new HashMap<>();
        message1.put("A", "VALUE-A");
        message1.put("B", "VALUE-B");
        message1.put("C", 1234567890L);
        message1.put("X", 1000L);
        message1.put("time", 1122334455L);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("A", "VALUE-A");
        message2.put("B", "VALUE-B");
        message2.put("C", 1234567890L);
        message2.put("X", 2500L);
        message2.put("time", 1122334555L);

        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("A", "VALUE-A");
        expected1.put("B", "VALUE-B");
        expected1.put("C", 1234567890L);
        expected1.put("time", 1122334455L);

        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("A", "VALUE-A");
        expected2.put("B", "VALUE-B");
        expected2.put("C", 1234567890L);
        expected2.put("X", 1500L);
        expected2.put("time", 1122334555L);
        expected2.put("last_timestamp", 1122334455L);

        KeyValue<String, Map<String, Object>> result1 = diffCounterStoreMapper.process(key1, message1);
        assertEquals(key1, result1.key);
        assertEquals(expected1, result1.value);

        KeyValue<String, Map<String, Object>> result2 = diffCounterStoreMapper.process(key1, message2);
        assertEquals(key1, result2.key);
        assertEquals(expected2, result2.value);
    }

    @Test
    public void sendZeroIfIsEnabled() {
        diffCounterStoreMapper.sendIfZero = true;

        String key1 = randomKey();
        Map<String, Object> message1 = new HashMap<>();
        message1.put("A", "VALUE-A");
        message1.put("B", "VALUE-B");
        message1.put("C", 1234567890L);
        message1.put("X", 1000L);
        message1.put("time", 1122334455L);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("A", "VALUE-A");
        message2.put("B", "VALUE-B");
        message2.put("C", 1234567890L);
        message2.put("X", 1000L);
        message2.put("time", 1122334655L);

        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("A", "VALUE-A");
        expected1.put("B", "VALUE-B");
        expected1.put("C", 1234567890L);
        expected1.put("time", 1122334455L);

        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("A", "VALUE-A");
        expected2.put("B", "VALUE-B");
        expected2.put("C", 1234567890L);
        expected2.put("X", 0L);
        expected2.put("time", 1122334655L);
        expected2.put("last_timestamp", 1122334455L);

        KeyValue<String, Map<String, Object>> result1 = diffCounterStoreMapper.process(key1, message1);
        assertEquals(key1, result1.key);
        assertEquals(expected1, result1.value);

        KeyValue<String, Map<String, Object>> result2 = diffCounterStoreMapper.process(key1, message2);
        assertEquals(key1, result2.key);
        assertEquals(expected2, result2.value);
    }

    @Test
    public void NotSendZeroIfIsEnabled() {
        diffCounterStoreMapper.sendIfZero = false;

        String key1 = randomKey();
        Map<String, Object> message1 = new HashMap<>();
        message1.put("A", "VALUE-A");
        message1.put("B", "VALUE-B");
        message1.put("C", 1234567890L);
        message1.put("X", 1000L);
        message1.put("time", 1122334455L);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("A", "VALUE-A");
        message2.put("B", "VALUE-B");
        message2.put("C", 1234567890L);
        message2.put("X", 1000L);
        message2.put("time", 1122334555L);

        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("A", "VALUE-A");
        expected1.put("B", "VALUE-B");
        expected1.put("C", 1234567890L);
        expected1.put("time", 1122334455L);


        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("A", "VALUE-A");
        expected2.put("B", "VALUE-B");
        expected2.put("C", 1234567890L);
        expected2.put("time", 1122334555L);
        expected2.put("last_timestamp", 1122334455L);

        KeyValue<String, Map<String, Object>> result1 = diffCounterStoreMapper.process(key1, message1);
        assertEquals(key1, result1.key);
        assertEquals(expected1, result1.value);

        KeyValue<String, Map<String, Object>> result2 = diffCounterStoreMapper.process(key1, message2);
        assertEquals(key1, result2.key);
        assertEquals(expected2, result2.value);
    }

    @Test
    public void sendDifferentsKeys() {
        String key1 = randomKey();
        String key2 = randomKey();

        Map<String, Object> message1 = new HashMap<>();
        message1.put("A", "VALUE-A");
        message1.put("B", "VALUE-B");
        message1.put("C", 1234567890L);
        message1.put("X", 1000L);
        message1.put("time", 1122334455L);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("A", "VALUE-A");
        message2.put("B", "VALUE-B");
        message2.put("C", 1234567890L);
        message2.put("X", 1000L);
        message2.put("time", 1122334655L);

        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("A", "VALUE-A");
        expected1.put("B", "VALUE-B");
        expected1.put("C", 1234567890L);
        expected1.put("time", 1122334455L);

        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("A", "VALUE-A");
        expected2.put("B", "VALUE-B");
        expected2.put("C", 1234567890L);
        expected2.put("time", 1122334655L);

        KeyValue<String, Map<String, Object>> result1 = diffCounterStoreMapper.process(key1, message1);
        assertEquals(key1, result1.key);
        assertEquals(expected1, result1.value);

        KeyValue<String, Map<String, Object>> result2 = diffCounterStoreMapper.process(key2, message2);
        assertEquals(key2, result2.key);
        assertEquals(expected2, result2.value);
    }

    @Test
    public void sendWithKeyDefinition() {
        String key1 = randomKey();
        String key2 = randomKey();

        Map<String, Object> message1 = new HashMap<>();
        message1.put("gateway", "gateway_1");
        message1.put("interface", "interface_1");
        message1.put("A", "VALUE-A");
        message1.put("B", "VALUE-B");
        message1.put("C", 1234567890L);
        message1.put("X", 1000L);
        message1.put("time", 1122334455L);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("gateway", "gateway_1");
        message2.put("interface", "interface_1");
        message2.put("A", "VALUE-A");
        message2.put("B", "VALUE-B");
        message2.put("C", 1234567890L);
        message2.put("X", 1000L);
        message2.put("time", 1122334655L);

        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("gateway", "gateway_1");
        expected1.put("interface", "interface_1");
        expected1.put("A", "VALUE-A");
        expected1.put("B", "VALUE-B");
        expected1.put("C", 1234567890L);
        expected1.put("time", 1122334455L);

        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("gateway", "gateway_1");
        expected2.put("interface", "interface_1");
        expected2.put("A", "VALUE-A");
        expected2.put("B", "VALUE-B");
        expected2.put("C", 1234567890L);
        expected2.put("X", 0L);
        expected2.put("time", 1122334655L);
        expected2.put("last_timestamp", 1122334455L);

        KeyValue<String, Map<String, Object>> result1 = diffCounterStoreMapper.process(key1, message1);
        assertEquals(key1, result1.key);
        assertEquals(expected1, result1.value);

        KeyValue<String, Map<String, Object>> result2 = diffCounterStoreMapper.process(key2, message2);
        assertEquals(key2, result2.key);
        assertEquals(expected2, result2.value);
    }

    @Test
    public void processMessageWithNullField() {
        String key1 = randomKey();

        Map<String, Object> message1 = new HashMap<>();
        message1.put("A", "VALUE-A");
        message1.put("B", "VALUE-B");
        message1.put("C", 1234567890L);
        message1.put("X", 2500L);
        message1.put("time", 1122334455L);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("A", "VALUE-A");
        message2.put("B", "VALUE-B");
        message2.put("C", 1234567890L);
        message2.put("time", 1122334655L);

        Map<String, Object> message3 = new HashMap<>();
        message3.put("A", "VALUE-A");
        message3.put("B", "VALUE-B");
        message3.put("C", 1234567890L);
        message3.put("X", 4000L);
        message3.put("time", 1122334855L);

        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("A", "VALUE-A");
        expected1.put("B", "VALUE-B");
        expected1.put("C", 1234567890L);
        expected1.put("time", 1122334455L);

        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("A", "VALUE-A");
        expected2.put("B", "VALUE-B");
        expected2.put("C", 1234567890L);
        expected2.put("time", 1122334655L);
        expected2.put("last_timestamp", 1122334455L);

        Map<String, Object> expected3 = new HashMap<>();
        expected3.put("A", "VALUE-A");
        expected3.put("B", "VALUE-B");
        expected3.put("C", 1234567890L);
        expected3.put("X", 1500L);
        expected3.put("time", 1122334855L);
        expected3.put("last_timestamp", 1122334655L);

        KeyValue<String, Map<String, Object>> result1 = diffCounterStoreMapper.process(key1, message1);
        assertEquals(key1, result1.key);
        assertEquals(expected1, result1.value);

        KeyValue<String, Map<String, Object>> result2 = diffCounterStoreMapper.process(key1, message2);
        assertEquals(key1, result2.key);
        assertEquals(expected2, result2.value);

        KeyValue<String, Map<String, Object>> result3 = diffCounterStoreMapper.process(key1, message3);
        assertEquals(key1, result3.key);
        assertEquals(expected3, result3.value);
    }

    @Test
    public void processMessageWithSecondNullTimestamp() {
        String key1 = randomKey();

        Map<String, Object> message1 = new HashMap<>();
        message1.put("A", "VALUE-A");
        message1.put("B", "VALUE-B");
        message1.put("C", 1234567890L);
        message1.put("X", 2500L);
        message1.put("time", 1122334455L);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("A", "VALUE-A");
        message2.put("B", "VALUE-B");
        message2.put("C", 1234567890L);

        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("A", "VALUE-A");
        expected1.put("B", "VALUE-B");
        expected1.put("C", 1234567890L);
        expected1.put("time", 1122334455L);

        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("A", "VALUE-A");
        expected2.put("B", "VALUE-B");
        expected2.put("C", 1234567890L);
        expected2.put("last_timestamp", 1122334455L);
        expected2.put("time", System.currentTimeMillis()/1000);


        KeyValue<String, Map<String, Object>> result1 = diffCounterStoreMapper.process(key1, message1);
        assertEquals(key1, result1.key);
        assertEquals(expected1, result1.value);

        KeyValue<String, Map<String, Object>> result2 = diffCounterStoreMapper.process(key1, message2);
        assertEquals(key1, result2.key);
        assertEquals(expected2, result2.value);
    }

    @Test
    public void processMessageWithoutTimestamp() {
        String key1 = randomKey();

        Map<String, Object> message1 = new HashMap<>();
        message1.put("A", "VALUE-A");
        message1.put("B", "VALUE-B");
        message1.put("C", 1234567890L);
        message1.put("X", 2500L);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("A", "VALUE-A");
        message2.put("B", "VALUE-B");
        message2.put("C", 1234567890L);

        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("A", "VALUE-A");
        expected1.put("B", "VALUE-B");
        expected1.put("C", 1234567890L);
        expected1.put("time", System.currentTimeMillis()/1000);

        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("A", "VALUE-A");
        expected2.put("B", "VALUE-B");
        expected2.put("C", 1234567890L);
        expected2.put("last_timestamp", System.currentTimeMillis()/1000);
        expected2.put("time", System.currentTimeMillis()/1000);


        KeyValue<String, Map<String, Object>> result1 = diffCounterStoreMapper.process(key1, message1);
        assertEquals(key1, result1.key);
        assertEquals(expected1, result1.value);

        KeyValue<String, Map<String, Object>> result2 = diffCounterStoreMapper.process(key1, message2);
        assertEquals(key1, result2.key);
        assertEquals(expected2, result2.value);
    }

    @Test
    public void processMessageWithFirstNullTimestamp() {
        String key1 = randomKey();

        Map<String, Object> message1 = new HashMap<>();
        message1.put("A", "VALUE-A");
        message1.put("B", "VALUE-B");
        message1.put("C", 1234567890L);
        message1.put("X", 2500L);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("A", "VALUE-A");
        message2.put("B", "VALUE-B");
        message2.put("C", 1234567890L);
        message2.put("time", System.currentTimeMillis()/1000 + 100);

        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("A", "VALUE-A");
        expected1.put("B", "VALUE-B");
        expected1.put("C", 1234567890L);
        expected1.put("time", System.currentTimeMillis()/1000);

        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("A", "VALUE-A");
        expected2.put("B", "VALUE-B");
        expected2.put("C", 1234567890L);
        expected2.put("last_timestamp", System.currentTimeMillis()/1000);
        expected2.put("time", System.currentTimeMillis()/1000 + 100);


        KeyValue<String, Map<String, Object>> result1 = diffCounterStoreMapper.process(key1, message1);
        assertEquals(key1, result1.key);
        assertEquals(expected1, result1.value);

        KeyValue<String, Map<String, Object>> result2 = diffCounterStoreMapper.process(key1, message2);
        assertEquals(key1, result2.key);
        assertEquals(expected2, result2.value);
    }

    @Test
    public void toStringTest(){
        assertNotNull(diffCounterStoreMapper);
        assertEquals(" {counters: [X], sendIfZero: true, stores: mock-key-value-store, timestamp: time, keys: [gateway, interface]} ",
                diffCounterStoreMapper.toString());
    }

    @AfterClass
    public static void stopTest(){
        streamBuilder.close();
    }

    private String randomKey(){
        return UUID.randomUUID().toString();
    }
}
