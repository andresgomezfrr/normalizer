package rb.ks;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import rb.ks.builder.StreamBuilder;
import rb.ks.exceptions.PlanBuilderException;
import rb.ks.exceptions.TryToDoLoopException;
import rb.ks.metrics.MetricsManager;
import rb.ks.model.PlanModel;

import java.io.File;
import java.io.IOException;

public class StreamBuilderTest {

    @Test(expected = TryToDoLoopException.class)
    public void tryToDoLoopExceptionTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("loop-stream.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        StreamBuilder streamBuilder = new StreamBuilder("APP-ID-1", null);
        streamBuilder.builder(model);

        streamBuilder.close();
    }
}
