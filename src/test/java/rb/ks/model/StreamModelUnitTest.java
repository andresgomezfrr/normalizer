package rb.ks.model;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamModelUnitTest {

    @Test
    public void shouldAssignFields() {

        List<FunctionModel> funcs = new ArrayList<>();
        TimestamperModel timestamperModelObject = mock(TimestamperModel.class);
        List<SinkModel> sinks = new ArrayList<>();

        StreamModel streamModelObject = new StreamModel(funcs, timestamperModelObject, sinks);

        assertNotNull(streamModelObject.getFuncs());
        assertEquals(streamModelObject.getFuncs(), funcs);

        assertNotNull(streamModelObject.getSinks());
        assertEquals(streamModelObject.getSinks(), sinks);

        assertNotNull(streamModelObject.getTimestamper());
        assertEquals(streamModelObject.getTimestamper(), timestamperModelObject);

    }


}
