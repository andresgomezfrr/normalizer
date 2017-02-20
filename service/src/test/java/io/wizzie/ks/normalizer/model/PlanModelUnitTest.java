package io.wizzie.ks.normalizer.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.ks.normalizer.builder.config.Config;
import io.wizzie.ks.normalizer.exceptions.MaxOutputKafkaTopics;
import io.wizzie.ks.normalizer.exceptions.PlanBuilderException;
import io.wizzie.ks.normalizer.serializers.JsonSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class PlanModelUnitTest {

    @Test
    public void throwExceptionInDuplicatedStreamsTest() {
        Map<String, List<String>> inputs = new HashMap<>();
        inputs.put("topic1", Arrays.asList("stream1", "stream1"));

        // StreamModel mock
        StreamModel streamModelMockObject = mock(StreamModel.class);

        Map<String, StreamModel> streams = new HashMap<>();
        streams.put("stream1", streamModelMockObject);

        PlanModel planModelObject = new PlanModel(inputs, streams);

        assertNotNull(planModelObject.getInputs());
        assertEquals(planModelObject.getInputs(), inputs);

        assertNotNull(planModelObject.getStreams());
        assertEquals(planModelObject.getStreams(), streams);

        try {
            Config config = new Config();
            planModelObject.validate(config);
            assertNotNull(planModelObject.getDefinedStreams());
        } catch (PlanBuilderException e) {
            assertEquals(e.getMessage(), "Stream[stream1]: Duplicated");
        }
    }

    @Test
    public void throwExceptionInNotDefinedInputStreamTest() {
        Map<String, List<String>> inputs = new HashMap<>();
        inputs.put("topic1", Arrays.asList("stream1", "stream2"));

        //StreamModel mock
        StreamModel streamModelObjectMock = mock(StreamModel.class);

        Map<String, StreamModel> streams = new HashMap<>();
        streams.put("notValidStream", streamModelObjectMock);

        PlanModel planModelObject = new PlanModel(inputs, streams);

        assertNotNull(planModelObject.getInputs());
        assertEquals(planModelObject.getInputs(), inputs);

        assertNotNull(planModelObject.getStreams());
        assertEquals(planModelObject.getStreams(), streams);

        try {
            Config config = new Config();
            planModelObject.validate(config);
            assertNotNull(planModelObject.getDefinedStreams());
        } catch (PlanBuilderException e) {
            assertEquals("Stream[notValidStream]: Not defined on inputs. Available definedStreams [stream1, stream2]", e.getMessage());
        }
    }

    @Test(expected = MaxOutputKafkaTopics.class)
    public void throwExceptionMaxOutputKafkaTopicsSingleStream() throws PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("multiple-kafka-sinks-1stream.json").getFile());
        ObjectMapper objectMapper = new ObjectMapper();

        PlanModel model = null;

        try {
            model = objectMapper.readValue(file, PlanModel.class);
        } catch (IOException e) {
            fail("Exception : " + e.getMessage());
        }

        Config config = new Config();
        config.put(Config.ConfigProperties.MAX_KAFKA_OUTPUT_TOPICS, 1);
        model.validate(config);
    }

    @Test(expected = MaxOutputKafkaTopics.class)
    public void throwExceptionMaxOutputKafkaTopicsMultipleStreams() throws PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("multiple-kafka-sinks-multiple-streams.json").getFile());
        ObjectMapper objectMapper = new ObjectMapper();

        PlanModel model = null;

        try {
            model = objectMapper.readValue(file, PlanModel.class);
        } catch (IOException e) {
            fail("Exception : " + e.getMessage());
        }

        Config config = new Config();
        config.put(Config.ConfigProperties.MAX_KAFKA_OUTPUT_TOPICS, 1);
        model.validate(config);
    }
}
