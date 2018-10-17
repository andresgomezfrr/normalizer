package io.wizzie.normalizer.funcs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.normalizer.base.utils.Constants;
import io.wizzie.normalizer.builder.StreamBuilder;
import io.wizzie.normalizer.exceptions.PlanBuilderException;
import io.wizzie.normalizer.funcs.FilterFunc;
import io.wizzie.normalizer.funcs.Function;
import io.wizzie.normalizer.model.PlanModel;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class FieldFilterUnitTest {
    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("field-filter.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myFilter");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        FieldFilter myFilter = (FieldFilter) myFunc;

        assertEquals("FILTER-DIMENSION", myFilter.dimension);
        assertFalse(myFilter.isDimensionKey);
        assertEquals("FILTER-VALUE", myFilter.dimensionValue);

        Function myFuncKey = functions.get("myFilterKey");

        assertNotNull(myFunc);
        assertTrue(myFuncKey instanceof FilterFunc);
        FieldFilter myFilterKey = (FieldFilter) myFuncKey;
        assertEquals(Constants.__KEY, myFilterKey.dimension);
        assertFalse(myFilter.isDimensionKey);
        assertEquals("FILTER-kEY", myFilterKey.dimensionValue);
    }

    @Test
    public void processSimpleMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myFilter");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        FieldFilter myFilter = (FieldFilter) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789L);
        message.put("FILTER-DIMENSION", "FILTER-VALUE");

        assertTrue(myFilter.process("key1", message));

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 123456789L);
        message1.put("FILTER-DIMENSION", "NOT-FILTER-VALUE");

        assertFalse(myFilter.process("key1", message1));
    }

    @Test
    public void processNullDimension() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myFilter");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        FieldFilter myFilter = (FieldFilter) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789L);

        assertFalse(myFilter.process("key1", message));
    }

    @Test
    public void processNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myFilter");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        FieldFilter myFilter = (FieldFilter) myFunc;

        Map<String, Object> message = null;

        assertFalse(myFilter.process("key1", message));
    }

    @Test
    public void processNullKey() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myFilterKey");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        FieldFilter myFilter = (FieldFilter) myFunc;

        Map<String, Object> message = new HashMap<>();

        assertFalse(myFilter.process(null, message));
    }


    @Test
    public void processFilterKey() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myFilterKey");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        FieldFilter myFilter = (FieldFilter) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789L);

        assertTrue(myFilter.process("FILTER-kEY", message));
        assertFalse(myFilter.process("NOT-kEY", message));
    }

    @Test
    public void processNullAsFilterValue() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myFilterNullAsValue");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        FieldFilter myFilter = (FieldFilter) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("FILTER-DIMENSION", null);

        assertTrue(myFilter.process(null, message));

        message.put("FILTER-DIMENSION", "NOT-NULL-VALUE");

        assertFalse(myFilter.process(null, message));
    }

    @AfterClass
    public static void stop() {
        streamBuilder.close();
    }
}
