package rb.ks.funcs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import rb.ks.builder.StreamBuilder;
import rb.ks.exceptions.PlanBuilderException;
import rb.ks.funcs.FilterFunc;
import rb.ks.funcs.Function;
import rb.ks.model.PlanModel;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ContainsDimensionFilterUnitTest {

    private static StreamBuilder streamBuilder = new StreamBuilder("app-id-1");

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("contains-filter.json").getFile());

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
        ContainsDimensionFilter myFilter = (ContainsDimensionFilter) myFunc;

        assertEquals(Arrays.asList("A", "B", "C"), myFilter.dimensions);
    }

    @Test
    public void processSimpleMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myFilter");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        ContainsDimensionFilter myFilter = (ContainsDimensionFilter) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("A", 123456789L);
        message.put("B", "FILTER-VALUE");
        message.put("C", "FILTER-VALUE");

        assertTrue(myFilter.process("key1", message));

        Map<String, Object> message1 = new HashMap<>();
        message1.put("A", 123456789L);
        message1.put("B", "NOT-FILTER-VALUE");

        assertFalse(myFilter.process("key1", message1));
    }

    @Test
    public void processNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myFilter");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        ContainsDimensionFilter myFilter = (ContainsDimensionFilter) myFunc;

        assertFalse(myFilter.process("key1", null));
    }


    @AfterClass
    public static void stop() {
        streamBuilder.close();
    }
}
