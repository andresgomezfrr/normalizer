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
import rb.ks.utils.Constants;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class FieldFilterUnitTest {

    private static StreamBuilder streamBuilder = new StreamBuilder("app-id-1", null);

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
