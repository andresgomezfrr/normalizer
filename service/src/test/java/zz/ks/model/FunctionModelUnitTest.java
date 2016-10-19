package zz.ks.model;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;


public class FunctionModelUnitTest {

    @Test
    public void functionModelIsBuiltCorrectly() {

        String className = "zz.ks.funcs.MyFunc";
        String name = "myFunctionName";
        Map<String, Object> properties = new HashMap<>();
        List<String> stores = new ArrayList<>();

        FunctionModel functionModelObject = new FunctionModel(name, className, properties, stores);

        assertNotNull(functionModelObject.getClassName());
        assertEquals(functionModelObject.getClassName(), className);

        assertNotNull(functionModelObject.getName());
        assertEquals(functionModelObject.getName(), name);

        assertNotNull(functionModelObject.getProperties());
        assertEquals(functionModelObject.getProperties(), properties);

        assertNotNull(functionModelObject.getStores());
        assertEquals(functionModelObject.getStores(), stores);

    }

}
