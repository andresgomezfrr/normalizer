package zz.ks.funcs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.matchers.And;
import zz.ks.builder.StreamBuilder;
import zz.ks.builder.config.Config;
import zz.ks.exceptions.PlanBuilderException;
import zz.ks.funcs.FilterFunc;
import zz.ks.funcs.Function;
import zz.ks.model.PlanModel;
import zz.ks.utils.Constants;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AndFilterUnitTest {
    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("and-filter.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myFilter");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof AndFilter);
        AndFilter myFilter = (AndFilter) myFunc;
        assertEquals(2, myFilter.filters.size());
    }

    @Test
    public void processBadSimpleMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myFilter");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof AndFilter);
        AndFilter myFilter = (AndFilter) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789L);
        message.put("FILTER-DIMENSION", "FILTER-VALUE");

        assertFalse(myFilter.process("key1", message));

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 123456789L);
        message1.put("FILTER-DIMENSION", "NOT-FILTER-VALUE");

        assertFalse(myFilter.process("key1", message1));
    }

    @Test
    public void processGoodSimpleMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myFilter");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof AndFilter);
        AndFilter myFilter = (AndFilter) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789L);
        message.put("FILTER-DIMENSION", "FILTER-VALUE");
        message.put("A", "A");
        message.put("B", "B");
        message.put("C", "C");

        assertTrue(myFilter.process("key1", message));
    }


    @AfterClass
    public static void stop(){
        streamBuilder.close();
    }
}
