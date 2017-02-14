package rb.ks.model;

import org.junit.Test;
import rb.ks.model.TimestamperModel;

import static org.junit.Assert.*;

public class TimeStamperModelUnitTest {

    @Test
    public void formatAndTimestampDimIsNotNullTest() {

        TimestamperModel timestamperModelObject = new TimestamperModel(null, null);

        assertNotNull(timestamperModelObject.getFormat());
        assertNotNull(timestamperModelObject.getTimestampDim());
    }

    @Test
    public void formatGenerateIsCorrect() {
        String timestamp = "timestamp";

        TimestamperModel timestamperModelObject = new TimestamperModel(timestamp, null);

        assertEquals(timestamperModelObject.generateTimestamp(new Object()), Long.valueOf(System.currentTimeMillis()/1000L));
    }

    @Test
    public void formatIsoIsCorrect() {
        String timestamp = "timestamp";
        String format = "iso";

        String date = "2016-09-08T06:33:46.000Z";

        TimestamperModel timestamperModelObject = new TimestamperModel(timestamp, format);

        assertEquals(timestamperModelObject.generateTimestamp(date), new Long(1473316426L));
    }

    @Test
    public void formatSecIsCorrect() {
        String timestamp = "timestamp";
        String format = "sec";

        String date = "1473316426";

        TimestamperModel timestamperModelObject = new TimestamperModel(timestamp, format);

        assertEquals(timestamperModelObject.generateTimestamp(date), new Long(1473316426L));
    }

    @Test
    public void formatMsIsCorrect() {
        String timestamp = "timestamp";
        String format = "ms";

        String date = "1473316426000";

        TimestamperModel timestamperModelObject = new TimestamperModel(timestamp, format);

        assertEquals(timestamperModelObject.generateTimestamp(date), new Long(1473316426L));
    }

}
