package unit;

import org.junit.Test;
import rb.ks.model.MapperModel;
import rb.ks.model.SinkModel;
import rb.ks.model.StreamModel;
import rb.ks.model.TimestamperModel;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class StreamModelUnitTest {

    @Test
    public void toStringIsCorrectTest() {

        // Prepare mocks
        MapperModel mapperModelObjectMock = mock(MapperModel.class);
        TimestamperModel timestamperObjectMock = mock(TimestamperModel.class);
        SinkModel sinkModelObjectMock = mock(SinkModel.class);

        when(mapperModelObjectMock.toString()).thenReturn("{dimPath: [A, B, C], as: X}");
        when(timestamperObjectMock.toString()).thenReturn("{timestampDim: timestamp, format: ms}");
        when(sinkModelObjectMock.toString()).thenReturn("{topic: output, partitionBy: Q}");

        // Create object
        StreamModel streamModelObject = new StreamModel(
                Arrays.asList(mapperModelObjectMock),
                timestamperObjectMock,
                Arrays.asList(sinkModelObjectMock));

        // Prepare expected string
        StringBuilder builder = new StringBuilder();

        builder
                .append("{mappers: [")
                .append(mapperModelObjectMock.toString())
                .append("], timestamper: ")
                .append(timestamperObjectMock.toString())
                .append(", sinks: [")
                .append(sinkModelObjectMock.toString())
                .append("]}");

        assertEquals(streamModelObject.toString(), builder.toString());

    }

}
