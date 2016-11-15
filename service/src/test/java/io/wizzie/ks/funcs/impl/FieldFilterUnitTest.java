package io.wizzie.ks.funcs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.ks.builder.StreamBuilder;
import io.wizzie.ks.builder.config.Config;
import io.wizzie.ks.exceptions.PlanBuilderException;
import io.wizzie.ks.funcs.FilterFunc;
import io.wizzie.ks.funcs.Function;
import io.wizzie.ks.model.PlanModel;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static io.wizzie.ks.utils.Constants.__KEY;
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
        assertEquals(__KEY, myFilterKey.dimension);
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

    @AfterClass
    public static void stop(){
        streamBuilder.close();
    }
}
