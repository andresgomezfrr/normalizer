package unit;

import org.junit.Test;

import rb.ks.exceptions.PlanBuilderException;
import rb.ks.model.*;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class PlanModelUnitTest {

    @Test
    public void stringIsCorrect () {

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
        }    }

    @Test
    public void throwExceptionInNotTimestampDimensionTest() {
        Map<String, List<String>> inputs = new HashMap<>();
        inputs.put("topic1", Arrays.asList("stream1", "stream2"));

        // StreamModel mock
        StreamModel streamModelObjectMock = mock(StreamModel.class);

        // MapperModel mock
        MapperModel mapperModelObjectMock = mock(MapperModel.class);
        // Set 'as' variable in MapperModel mock
        mapperModelObjectMock.as = "validTimestampDimension";

        // TimestamperModel mock
        TimestamperModel timestamperModelObjectMock = mock(TimestamperModel.class);

        List<MapperModel> mapperModelListObject = Collections.singletonList(mapperModelObjectMock);

        // When call 'getMappers' method from StreamModel mock return object list with MapperModel mock
        when(streamModelObjectMock.getMappers()).thenReturn(mapperModelListObject);
        // When call 'getTimestamper' method from StreamModel mock return TimestamperModel mock
        when(streamModelObjectMock.getTimestamper()).thenReturn(timestamperModelObjectMock);

        // When call 'getTimestampDim' method from TimestamperModel mock return "notDefinedDimension" value
        when(timestamperModelObjectMock.getTimestampDim()).thenReturn("notDefinedDimension");
        // When call 'getFormat' method from TimerstamperModel mock return "iso" value
        when(timestamperModelObjectMock.getFormat()).thenReturn("iso");

        Map<String, StreamModel> streams = new HashMap<>();
        streams.put("stream1", streamModelObjectMock);

        PlanModel planModelObject = new PlanModel(inputs, streams);

        try {
            planModelObject.validate();
        } catch (PlanBuilderException e) {
            assertEquals(e.getMessage(), "Stream[stream1]: Timestamp dimension [notDefinedDimension] is not on the message. Available dimensions [validTimestampDimension]");
        }
    }

    @Test//(expected = PlanBuilderException.class)
    public void throwExceptionInNotPartitionByDimensionTest() {
        Map<String, List<String>> inputs = new HashMap<>();
        inputs.put("topic1", Arrays.asList("stream1", "stream2"));

        // StreamModel mock
        StreamModel streamModelObjectMock = mock(StreamModel.class);

        // MapperModel mock
        MapperModel mapperModelObjectMock = mock(MapperModel.class);
        // Set 'as' variable in MapperModel mock
        mapperModelObjectMock.as = "timestamp";

        // MapperModel mock
        MapperModel mapperModelObjectMock2 = mock(MapperModel.class);
        // Set 'as' variable in MapperModel mock
        mapperModelObjectMock2.as = "Q";

        // SinkModel mock
        SinkModel sinkModelObjectMock = mock(SinkModel.class);

        // TimestamperModel mock
        TimestamperModel timestamperModelObjectMock = mock(TimestamperModel.class);

        List<MapperModel> mapperModelListObject = Arrays.asList(mapperModelObjectMock, mapperModelObjectMock2);

        // When call 'getPartitionBy' method from SinkModel mock return "J" value
        when(sinkModelObjectMock.getPartitionBy()).thenReturn("J");

        // When call 'getMappers' from StreamModel mock return list of MapperModel mocks
        when(streamModelObjectMock.getMappers()).thenReturn(mapperModelListObject);
        // When call 'getTimestamper' from StreamModel mock return TimestamperModel mock
        when(streamModelObjectMock.getTimestamper()).thenReturn(timestamperModelObjectMock);
        // When call 'getSinks' from StreamModel mock return list of SinkModel mock
        when(streamModelObjectMock.getSinks()).thenReturn(Collections.singletonList(sinkModelObjectMock));
        // When call 'getTimestampDim' from TimestamperModel mock return "timestamp" value
        when(timestamperModelObjectMock.getTimestampDim()).thenReturn("timestamp");
        // When call 'getFormat' from TimestamperModel mock return "iso" value
        when(timestamperModelObjectMock.getFormat()).thenReturn("iso");

        Map<String, StreamModel> streams = new HashMap<>();
        streams.put("stream1", streamModelObjectMock);

        PlanModel planModelObject = new PlanModel(inputs, streams);

        try {
            planModelObject.validate();
        } catch (PlanBuilderException e) {
            assertEquals(e.getMessage(), "Stream[stream1]: PartitionBy dimension [J] is not on the message. Available dimensions [Q, timestamp]");
        }

    }


}
