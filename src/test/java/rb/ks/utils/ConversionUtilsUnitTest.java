package rb.ks.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConversionUtilsUnitTest {

    @Test
    public void conversionFromStringToLongObjectIsCorrectTest() {
        String number = "1234567890";

        assertEquals(ConversionUtils.toLong(number), new Long(1234567890L));
    }

    @Test
    public void conversionFromIntegerToLongObjectIsCorrectTest() {
        int number = 1234567890;

        assertEquals(ConversionUtils.toLong(number), new Long(1234567890L));
    }

    @Test
    public void conversionFromLongToLongObjectIsCorrectTest() {
        long number = 1234567890L;

        assertEquals(ConversionUtils.toLong(number), new Long(number));
    }

}
