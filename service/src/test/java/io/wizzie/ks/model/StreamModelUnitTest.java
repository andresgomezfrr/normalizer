package io.wizzie.ks.model;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StreamModelUnitTest {

    @Test
    public void shouldAssignFields() {

        List<FunctionModel> funcs = new ArrayList<>();
        List<SinkModel> sinks = new ArrayList<>();

        StreamModel streamModelObject = new StreamModel(funcs, sinks);

        assertNotNull(streamModelObject.getFuncs());
        assertEquals(streamModelObject.getFuncs(), funcs);

        assertNotNull(streamModelObject.getSinks());
        assertEquals(streamModelObject.getSinks(), sinks);
    }


}
