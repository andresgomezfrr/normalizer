package io.wizzie.ks.normalizer.funcs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.ks.normalizer.builder.StreamBuilder;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.ks.normalizer.exceptions.PlanBuilderException;
import io.wizzie.ks.normalizer.funcs.Function;
import io.wizzie.ks.normalizer.model.PlanModel;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class SimpleMapperUnitTest {
    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("simple-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleMapper);
        SimpleMapper myMapper = (SimpleMapper) myFunc;

        assertEquals(Arrays.asList("A", "B", "C"), myMapper.mappers.get(0).getDimPath());
        assertEquals("X", myMapper.mappers.get(0).getAs());

        assertEquals(Arrays.asList("Y", "W", "Z"), myMapper.mappers.get(1).getDimPath());
        assertEquals("Q", myMapper.mappers.get(1).getAs());

        assertEquals(Arrays.asList("Y", "W", "P"), myMapper.mappers.get(2).getDimPath());
        assertEquals("P", myMapper.mappers.get(2).getAs());

        assertEquals(Arrays.asList("timestamp"), myMapper.mappers.get(3).getDimPath());
        assertEquals("timestamp", myMapper.mappers.get(3).getAs());
    }

    @Test
    public void processSimpleMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleMapper);
        SimpleMapper myMapper = (SimpleMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);

        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        b.put("C", "TEST-C");
        a.put("B", b);
        message.put("A", a);


        Map<String, Object> y = new HashMap<>();
        Map<String, Object> w = new HashMap<>();
        w.put("Z", "TEST-Z");
        w.put("P", "TEST-P");
        y.put("W", w);
        message.put("Y", y);

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process("key1", message);
        assertEquals("key1", mapMessage.key);

        Map<String, Object> value = mapMessage.value;
        assertEquals("TEST-C", value.get("X"));
        assertEquals("TEST-Z", value.get("Q"));
        assertEquals("TEST-P", value.get("P"));
        assertEquals(123456789, value.get("timestamp"));
    }

    @Test
    public void processNullKey() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleMapper);
        SimpleMapper myMapper = (SimpleMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);

        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        b.put("C", "TEST-C");
        a.put("B", b);
        message.put("A", a);

        Map<String, Object> y = new HashMap<>();
        Map<String, Object> w = new HashMap<>();
        w.put("Z", "TEST-Z");
        w.put("P", "TEST-P");
        y.put("W", w);
        message.put("Y", y);

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process(null, message);
        assertNull(mapMessage.key);

        Map<String, Object> value = mapMessage.value;
        assertEquals("TEST-C", value.get("X"));
        assertEquals("TEST-Z", value.get("Q"));
        assertEquals("TEST-P", value.get("P"));
        assertEquals(123456789, value.get("timestamp"));
    }

    @Test
    public void processNullKeyAndNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleMapper);
        SimpleMapper myMapper = (SimpleMapper) myFunc;

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process(null, null);
        assertNull(mapMessage.key);
        assertNull(mapMessage.value);
    }

    @Test
    public void toStringTest(){
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertEquals("[ {dimPath: [A, B, C], as: X}  {dimPath: [Y, W, Z], as: Q} " +
                " {dimPath: [Y, W, P], as: P}  {dimPath: [timestamp], as: timestamp} ]", myFunc.toString());
    }

    @AfterClass
    public static void stopTest(){
        streamBuilder.close();
    }
}
