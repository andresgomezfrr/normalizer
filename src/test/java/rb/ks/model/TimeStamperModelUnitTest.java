package rb.ks.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TimeStamperModelUnitTest {

    @Test
    public void formatAndTimestampDimIsNotNullTest() {

        TimestamperModel timestamperModelObject = new TimestamperModel(null, null);

        assertNotNull(timestamperModelObject.getFormat());
        assertEquals(timestamperModelObject.getFormat(), "none");

        assertNotNull(timestamperModelObject.getTimestampDim());
        assertEquals(timestamperModelObject.getTimestampDim(), "timestamp");

    }

    @Test
    public void formatGenerateIsCorrect() {
        String timestamp = "timestamp";

        TimestamperModel timestamperModelObject = new TimestamperModel(timestamp, null);

        assertNotNull(timestamperModelObject.getTimestampDim());
        assertEquals(timestamperModelObject.getTimestampDim(), timestamp);

        assertNotNull("none", timestamperModelObject.getFormat());
        assertNull(timestamperModelObject.generateTimestamp(new Object()));

    }

    @Test
    public void formatIsoIsCorrect() {
        String timestamp = "timestamp";
        String format = "iso";

        String date = "2016-09-08T06:33:46.000Z";

        TimestamperModel timestamperModelObject = new TimestamperModel(timestamp, format);

        assertNotNull(timestamperModelObject.format);
        assertEquals(timestamperModelObject.getFormat(), format);

        assertNotNull(timestamperModelObject.timestampDim);
        assertEquals(timestamperModelObject.getTimestampDim(), timestamp);

        assertEquals(timestamperModelObject.generateTimestamp(date), new Long(1473316426L));

    }

    @Test
    public void formatSecIsCorrect() {
        String timestamp = "timestamp";
        String format = "sec";

        String date = "1473316426";

        TimestamperModel timestamperModelObject = new TimestamperModel(timestamp, format);

        assertNotNull(timestamperModelObject.getFormat());
        assertEquals(timestamperModelObject.getFormat(), format);

        assertNotNull(timestamperModelObject.getTimestampDim());
        assertEquals(timestamperModelObject.getTimestampDim(), timestamp);

        assertEquals(timestamperModelObject.generateTimestamp(date), new Long(1473316426L));
    }

    @Test
    public void formatMsIsCorrect() {
        String timestamp = "timestamp";
        String format = "ms";

        String date = "1473316426000";

        TimestamperModel timestamperModelObject = new TimestamperModel(timestamp, format);

        assertNotNull(timestamperModelObject.getFormat());
        assertEquals(timestamperModelObject.getFormat(), format);

        assertNotNull(timestamperModelObject.getTimestampDim());
        assertEquals(timestamperModelObject.getTimestampDim(), timestamp);

        assertEquals(timestamperModelObject.generateTimestamp(date), new Long(1473316426L));
    }

}
