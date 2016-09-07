package rb.ks.utils;

public class ConversionUtils {
    public static Long toLong(Object l) {
        Long result = null;

        try {
            if (l != null) {
                if (l instanceof Integer) {
                    result = ((Integer) l).longValue();
                } else if (l instanceof Long) {
                    result = (Long) l;
                } else if (l instanceof String) {
                    result = Long.valueOf((String) l);
                }
            }
        } catch (NumberFormatException ex) {
            // TODO: Log throw exception.
        }

        return result;
    }
}
