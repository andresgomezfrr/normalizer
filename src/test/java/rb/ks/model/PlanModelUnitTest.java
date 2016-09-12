package rb.ks.model;

import org.junit.Test;

import rb.ks.exceptions.PlanBuilderException;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class PlanModelUnitTest {

    @Test
    public void stringIsCorrect() {

    }

    @Test
    public void throwExceptionInDuplicatedStreamsTest() {
        Map<String, List<String>> inputs = new HashMap<>();
        inputs.put("topic1", Arrays.asList("stream1", "stream1"));

        // StreamModel mock
        StreamModel streamModelObjectMock = mock(StreamModel.class);

        Map<String, StreamModel> streams = new HashMap<>();
        streams.put("stream1", streamModelObjectMock);

        PlanModel planModelObject = new PlanModel(inputs, streams);

        try {
            planModelObject.validate();
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

        try {
            planModelObject.validate();
        } catch (PlanBuilderException e) {
            assertEquals(e.getMessage(), "Stream[notValidStream]: Not defined on inputs. Available inputs {topic1=[stream1, stream2]}");
        }
    }

}
