package rb.ks.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rb.ks.Normalizer;
import rb.ks.utils.ConversionUtils;

public class TimestamperModel {
    private static final Logger log = LoggerFactory.getLogger(TimestamperModel.class);

    String timestampDim = "timestamp";
    String format = "generate";
    DateTimeFormatter patternFormat = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
            .appendTimeZoneOffset("Z", true, 2, 4)
            .toFormatter();

    @JsonCreator
    public TimestamperModel(@JsonProperty("dimension") String timestampDim,
                            @JsonProperty("format") String format) {
        if (timestampDim != null) this.timestampDim = timestampDim;
        if (format != null) this.format = format;
    }


    @JsonProperty
    public String getTimestampDim() {
        return timestampDim;
    }

    @JsonProperty
    public String getFormat() {
        return format;
    }

    public Long generateTimestamp(Object time) {
        Long timestamp = null;

        if (time != null) {
            switch (format) {
                case "generate":
                    timestamp = System.currentTimeMillis() / 1000L;
                    break;
                case "ms":
                    timestamp = ConversionUtils.toLong(time) / 1000L;
                    break;
                case "sec":
                    timestamp = ConversionUtils.toLong(time);
                    break;
                case "iso":
                    timestamp = patternFormat.parseDateTime((String) time).getMillis() / 1000L;
                    break;
                default:
                    log.warn("Log format not valid, used 'generate'");
                    timestamp = System.currentTimeMillis() / 1000L;
                    break;
            }
        } else {
            timestamp = System.currentTimeMillis() / 1000L;
            log.warn("Timestamp is null!!");
        }

        return timestamp;
    }


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append("timestampDim: ").append(timestampDim).append(", ")
                .append("format: ").append(format)
                .append("}");

        return builder.toString();
    }
}
