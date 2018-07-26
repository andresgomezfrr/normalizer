package io.wizzie.normalizer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.normalizer.builder.StreamBuilder;
import io.wizzie.normalizer.exceptions.PlanBuilderException;
import io.wizzie.normalizer.exceptions.TryToDoLoopException;
import io.wizzie.normalizer.model.PlanModel;
import io.wizzie.normalizer.exceptions.PlanBuilderException;
import io.wizzie.normalizer.model.PlanModel;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class StreamBuilderTest {

    @Test(expected = TryToDoLoopException.class)
    public void tryToDoLoopExceptionTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("loop-stream.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);

        Config config = new Config();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "APP-ID-1");

        StreamBuilder streamBuilder = new StreamBuilder(config, null);
        streamBuilder.builder(model);

        streamBuilder.close();
    }
}
