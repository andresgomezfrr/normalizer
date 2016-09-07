package unit;

import org.junit.Test;
import rb.ks.model.SinkModel;

import static org.junit.Assert.*;

public class SinkModelUnitTest {

    @Test
    public void partitionByIsNotNullTest() {
        String topic = "output";
        String partitionBy = "Q";

        SinkModel sinkModelObject = new SinkModel(topic, partitionBy);

        assertEquals(sinkModelObject.getPartitionBy(), partitionBy);
    }

    @Test
    public void partitionByIsNullTest() {
        String topic = "output1";

        SinkModel sinkModelObject = new SinkModel(topic, null);

        assertEquals(sinkModelObject.getPartitionBy(), "__KEY");
    }

    @Test
    public void stringIsCorrectTest() {
        String topic = "output";
        String partitionBy = "Q";

        SinkModel sinkModelObject = new SinkModel(topic, partitionBy);

        assertEquals(sinkModelObject.toString(), "{topic: output, partitionBy: Q}");
    }

}
