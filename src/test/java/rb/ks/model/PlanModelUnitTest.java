package rb.ks.model;

import org.junit.Assert;
import org.junit.Test;
import rb.ks.exceptions.PlanBuilderException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
            planModelObject.validate();
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
            planModelObject.validate();
            assertNotNull(planModelObject.getDefinedStreams());
        } catch (PlanBuilderException e) {
            assertEquals(e.getMessage(), "Stream[notValidStream]: Not defined on inputs. Available inputs {topic1=[stream1, stream2]}");
        }
    }

    @Test
    public void avoidNullParameters() {
        PlanModel planModelObject = new PlanModel(null, null);

        assertNotNull(planModelObject.getInputs());
        assertNotNull(planModelObject.getStreams());

        assertTrue(planModelObject.getInputs().isEmpty());
        assertTrue(planModelObject.getStreams().isEmpty());

        try {
            planModelObject.validate();
        } catch (PlanBuilderException e) {
            e.printStackTrace();
        }
    }

}
