package rb.ks.funcs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KeyValue;
import org.junit.BeforeClass;
import org.junit.Test;
import rb.ks.builder.StreamBuilder;
import rb.ks.exceptions.PlanBuilderException;
import rb.ks.funcs.FlatMapperFunction;
import rb.ks.funcs.Function;
import rb.ks.model.PlanModel;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class ArrayFlattenMapperUnitTest {

    private static StreamBuilder streamBuilder = new StreamBuilder("app-id-1", null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("array-flatten-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {

        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myArrayFlatMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FlatMapperFunction);
        ArrayFlattenMapper myFilter = (ArrayFlattenMapper) myFunc;
        assertEquals("ARRAY", myFilter.flatDimension);
    }

    @Test
    public void simpleArrayProcess() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myArrayFlatMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FlatMapperFunction);
        ArrayFlattenMapper myFilter = (ArrayFlattenMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("A", "VALUE-A");
        message.put("B", "VALUE-B");
        message.put("C", 12345);
        message.put("ARRAY", Arrays.asList("X", "Y", "Z"));

        List<KeyValue<String, Map<String, Object>>> expectedResult = new ArrayList<>();

        Map<String, Object> expected = new HashMap<>();
        expected.put("A", "VALUE-A");
        expected.put("B", "VALUE-B");
        expected.put("C", 12345);
        expected.put("ARRAY", "X");

        expectedResult.add(new KeyValue<>("KEY_1", expected));

        expected = new HashMap<>();
        expected.put("A", "VALUE-A");
        expected.put("B", "VALUE-B");
        expected.put("C", 12345);
        expected.put("ARRAY", "Y");

        expectedResult.add(new KeyValue<>("KEY_1", expected));

        expected = new HashMap<>();
        expected.put("A", "VALUE-A");
        expected.put("B", "VALUE-B");
        expected.put("C", 12345);
        expected.put("ARRAY", "Z");

        expectedResult.add(new KeyValue<>("KEY_1", expected));

        List<KeyValue<String, Map<String, Object>>> result = (List<KeyValue<String, Map<String, Object>>>) myFilter.process("KEY_1", message);

        assertEquals(expectedResult, result);
    }

    @Test
    public void notDimensionProcess() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myArrayFlatMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FlatMapperFunction);
        ArrayFlattenMapper myFilter = (ArrayFlattenMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("A", "VALUE-A");
        message.put("B", "VALUE-B");
        message.put("C", 12345);

        Map<String, Object> expected = new HashMap<>();
        expected.put("A", "VALUE-A");
        expected.put("B", "VALUE-B");
        expected.put("C", 12345);

        List<KeyValue<String, Map<String, Object>>> result = (List<KeyValue<String, Map<String, Object>>>) myFilter.process("KEY_1", message);

        assertEquals(Collections.singletonList(new KeyValue<>("KEY_1", expected)), result);
    }

    @Test
    public void nullArrayProcess() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myArrayFlatMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FlatMapperFunction);
        ArrayFlattenMapper myFilter = (ArrayFlattenMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("A", "VALUE-A");
        message.put("B", "VALUE-B");
        message.put("C", 12345);
        message.put("ARRAY", null);

        Map<String, Object> expected = new HashMap<>();
        expected.put("A", "VALUE-A");
        expected.put("B", "VALUE-B");
        expected.put("C", 12345);

        List<KeyValue<String, Map<String, Object>>> result = (List<KeyValue<String, Map<String, Object>>>) myFilter.process("KEY_1", message);

        assertEquals(Collections.singletonList(new KeyValue<>("KEY_1", expected)), result);
    }
}
