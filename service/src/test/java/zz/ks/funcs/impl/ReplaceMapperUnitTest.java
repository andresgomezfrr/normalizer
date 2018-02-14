package zz.ks.funcs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import zz.ks.builder.StreamBuilder;
import zz.ks.builder.config.Config;
import zz.ks.exceptions.PlanBuilderException;
import zz.ks.funcs.Function;
import zz.ks.funcs.MapperFunction;
import zz.ks.model.PlanModel;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ReplaceMapperUnitTest {

    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("replace-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myReplaceMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        ReplaceMapper myReplaceMapper = (ReplaceMapper) myFunc;

        assertEquals("TYPE", myReplaceMapper.dimension);
        assertNotNull(myReplaceMapper.replacements);

        Map<String, String> replacements = new HashMap<>();
        replacements.put("v", "version");
        replacements.put("ver", "version");
        replacements.put("vrsn", "version");

        assertEquals(replacements, myReplaceMapper.replacements);
    }

    @Test
    public void processSimpleMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myReplaceFunc = functions.get("myReplaceMapper");

        assertNotNull(myReplaceFunc);
        assertTrue(myReplaceFunc instanceof MapperFunction);
        MapperFunction myReplaceMapper = (MapperFunction) myReplaceFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 123456789L);
        message1.put("TYPE", "ver");
        message1.put("A", "VALUE-A");

        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 123456789L);
        expectedMessage1.put("TYPE", "version");
        expectedMessage1.put("A", "VALUE-A");

        KeyValue<String, Map<String, Object>> result1 = myReplaceMapper.process("KEY", message1);

        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("timestamp", 123456789L);
        message2.put("TYPE", "vrsn");
        message2.put("B", "VALUE-B");

        Map<String, Object> expectedMessage2 = new HashMap<>();
        expectedMessage2.put("timestamp", 123456789L);
        expectedMessage2.put("TYPE", "version");
        expectedMessage2.put("B", "VALUE-B");

        KeyValue<String, Map<String, Object>> result2 = myReplaceMapper.process("KEY", message2);

        assertEquals(new KeyValue<>("KEY", expectedMessage2), result2);
    }

    @Test
    public void processNullDimension() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myReplaceFunc = functions.get("myReplaceMapper");

        assertNotNull(myReplaceFunc);
        assertTrue(myReplaceFunc instanceof MapperFunction);
        MapperFunction myReplaceMapper = (MapperFunction) myReplaceFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789L);
        message.put("TYPE", null);
        message.put("A", "VALUE-A");

        KeyValue<String, Map<String, Object>> result = myReplaceMapper.process("KEY", message);

        assertEquals(new KeyValue<>("KEY", message), result);
    }

    @AfterClass
    public static void stop(){
        streamBuilder.close();
    }

}
