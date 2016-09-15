package rb.ks.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SinkModelUnitTest {

    @Test
    public void partitionByIsNotNullTest() {
        String topic = "output";
        String partitionBy = "Q";

        SinkModel sinkModelObject = new SinkModel(topic, partitionBy);

        assertNotNull(sinkModelObject.topic);
        assertEquals(sinkModelObject.getTopic(), topic);

        assertNotNull(sinkModelObject.partitionBy);
        assertEquals(sinkModelObject.getPartitionBy(), partitionBy);
    }

    @Test
    public void partitionByIsNullTest() {
        String topic = "output1";

        SinkModel sinkModelObject = new SinkModel(topic, null);

        assertNotNull(sinkModelObject.topic);
        assertEquals(sinkModelObject.getTopic(), topic);

        assertNotNull(sinkModelObject.partitionBy);
        assertEquals(sinkModelObject.getPartitionBy(), "__KEY");
    }

    @Test
    public void stringIsCorrectTest() {
        String topic = "output2";
        String partitionBy = "N";

        SinkModel sinkModelObject = new SinkModel(topic, partitionBy);
        assertNotNull(sinkModelObject.topic);
        assertEquals(sinkModelObject.getTopic(), topic);

        assertNotNull(sinkModelObject.partitionBy);
        assertEquals(sinkModelObject.getPartitionBy(), partitionBy);

        assertEquals(sinkModelObject.toString(), "{topic: output2, partitionBy: N}");
    }

}
