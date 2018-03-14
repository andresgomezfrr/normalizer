package unit;

import org.junit.Test;
import rb.ks.model.MapperModel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class MapperModelUnitTest {

    @Test
    public void asIsNotNullTest() {

        List<String> dimPaths = Arrays.asList("A", "B", "C");
        String as = "X";

        MapperModel mapperModelObject = new MapperModel(dimPaths, as);

        assertEquals(mapperModelObject.getAs(), as);
    }

    @Test
    public void asIsNullTest() {

        List<String> dimPaths = Arrays.asList("Y", "W", "Z");

        MapperModel mapperModelObject = new MapperModel(dimPaths, null);

        assertEquals(mapperModelObject.getAs(), "Z");
    }

    @Test
    public void stringIsCorrectTest() {
        List<String> dimPaths = Arrays.asList("I", "J", "K");

        MapperModel mapperModelObject = new MapperModel(dimPaths, null);

        assertEquals(mapperModelObject.toString(), "{dimPath: [I, J, K], as: K}");
    }
}
