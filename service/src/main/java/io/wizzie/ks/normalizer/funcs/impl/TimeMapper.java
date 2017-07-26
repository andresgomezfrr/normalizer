package io.wizzie.ks.normalizer.funcs.impl;

import io.wizzie.ks.normalizer.exceptions.FunctionException;
import io.wizzie.ks.normalizer.funcs.MapperFunction;
import io.wizzie.ks.normalizer.metrics.MetricsManager;
import io.wizzie.ks.normalizer.utils.ConversionUtils;
import org.apache.kafka.streams.KeyValue;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class TimeMapper extends MapperFunction {

    public static final String FROM_FORMAT = "fromFormat";
    public static final String DIMENSION = "dimension";
    public static final String TO_FORMAT = "toFormat";
    public static final String FORCE_STRING_OUTPUT = "forceStringOutput";

    private final String ERROR_MESSAGE_PATTERN = "%s cannot be null";


    String dimensionToProcess;
    String fromFormat;
    String toFormat;
    DateTimeFormatter fmtISO;
    Function<Object, Object> convertTime;
    boolean forceOutputString;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        dimensionToProcess = checkNotNull((String) properties.getOrDefault(DIMENSION, ""), String.format(ERROR_MESSAGE_PATTERN, DIMENSION));
        fromFormat = checkNotNull((String) properties.getOrDefault(FROM_FORMAT, ""), String.format(ERROR_MESSAGE_PATTERN, FROM_FORMAT));
        toFormat = checkNotNull((String) properties.getOrDefault(TO_FORMAT, ""), String.format(ERROR_MESSAGE_PATTERN, TO_FORMAT));
        forceOutputString = checkNotNull(new Boolean((String) properties.getOrDefault(FORCE_STRING_OUTPUT, "false")), String.format(ERROR_MESSAGE_PATTERN, FORCE_STRING_OUTPUT));

        if (!(fromFormat.equals("ISO") || fromFormat.equals("millis") || fromFormat.equals("secs") || fromFormat.startsWith("pattern:"))) {
            throw new FunctionException(FROM_FORMAT + " at TimeMapper must be 'ISO', 'millis', 'secs' or start with 'pattern:'");
        }

        if (!(toFormat.equals("ISO") || toFormat.equals("millis") || toFormat.equals("secs") || toFormat.startsWith("pattern:"))) {
            throw new FunctionException(FROM_FORMAT + " at TimeMapper must be 'ISO', 'millis', 'secs' or start with 'pattern:'");
        }

        DateTimeZone zoneUTC = DateTimeZone.UTC;
        DateTimeZone.setDefault(zoneUTC);

        if (fromFormat.equals("ISO") || toFormat.equals("ISO")) {
            fmtISO = ISODateTimeFormat.dateTime();
        }

        if (toFormat.equals("secs") && fromFormat.equals("millis")) {
            convertTime = (k) -> {
                if (k instanceof String) {
                    k = Long.parseLong((String) k);
                }
                return ConversionUtils.toLong(k) / 1000L;
            };
        } else if (toFormat.equals("millis") && fromFormat.equals("secs")) {
            convertTime = (k) -> ConversionUtils.toLong(k) * 1000L;
        } else if (toFormat.equals("ISO") && fromFormat.equals("secs")) {
            convertTime = (k) -> {
                DateTime dt = new DateTime(ConversionUtils.toLong(k) * 1000L);
                return dt.toDateTimeISO().toString();
            };
        } else if (toFormat.equals("ISO") && fromFormat.equals("millis")) {
            convertTime = (k) -> {
                if (k instanceof String) {
                    k = Long.parseLong((String) k);
                }
                DateTime dt = new DateTime(ConversionUtils.toLong(k));
                return dt.toDateTimeISO().toString();
            };
        } else if (toFormat.equals("millis") && fromFormat.equals("ISO")) {
            convertTime = (k) -> {
                DateTime dt = new DateTime(k);
                return fmtISO.parseMillis(dt.toDateTimeISO().toString());
            };
        } else if (toFormat.equals("secs") && fromFormat.equals("ISO")) {
            convertTime = (k) -> {
                DateTime dt = new DateTime(k);
                return fmtISO.parseMillis(dt.toDateTimeISO().toString()) / 1000L;
            };
        } else if (toFormat.startsWith("pattern:") && fromFormat.equals("ISO")) {
            toFormat = toFormat.split("pattern:")[1].trim();
            convertTime = (k) -> {
                DateTime dt = new DateTime(k);
                DateTimeFormatter fmtPattern = DateTimeFormat.forPattern(toFormat);
                return fmtPattern.print(dt);
            };
        } else if (toFormat.startsWith("pattern:") && fromFormat.equals("millis")) {
            toFormat = toFormat.split("pattern:")[1].trim();
            convertTime = (k) -> {
                if (k instanceof String) {
                    k = Long.parseLong((String) k);
                }
                DateTime dt = new DateTime(ConversionUtils.toLong(k));
                DateTimeFormatter fmtPattern = DateTimeFormat.forPattern(toFormat);
                return fmtPattern.print(dt);
            };
        } else if (toFormat.startsWith("pattern:") && fromFormat.equals("secs")) {
            toFormat = toFormat.split("pattern:")[1].trim();
            convertTime = (k) -> {
                DateTime dt = new DateTime(ConversionUtils.toLong(k) * 1000L);
                DateTimeFormatter fmtPattern = DateTimeFormat.forPattern(toFormat);
                return fmtPattern.print(dt);
            };
        } else if (toFormat.startsWith("pattern:") && fromFormat.startsWith("pattern:")) {
            toFormat = toFormat.split("pattern:")[1].trim();
            fromFormat = fromFormat.split("pattern:")[1].trim();
            convertTime = (k) -> {
                DateTimeFormatter fmtToPattern = DateTimeFormat.forPattern(toFormat);
                DateTimeFormatter fmtFromPattern = DateTimeFormat.forPattern(fromFormat);
                DateTime dt = fmtFromPattern.parseDateTime(k.toString());
                return fmtToPattern.print(dt);
            };
        } else if (toFormat.equals("millis") && fromFormat.startsWith("pattern:")) {
            fromFormat = fromFormat.split("pattern:")[1].trim();
            convertTime = (k) -> {
                DateTimeFormatter fmtPattern = DateTimeFormat.forPattern(fromFormat);
                DateTime dt = fmtPattern.parseDateTime(k.toString());
                return dt.toInstant().getMillis();
            };
        } else if (toFormat.equals("secs") && fromFormat.startsWith("pattern:")) {
            fromFormat = fromFormat.split("pattern:")[1].trim();
            convertTime = (k) -> {
                DateTimeFormatter fmtPattern = DateTimeFormat.forPattern(fromFormat);
                DateTime dt = fmtPattern.parseDateTime(k.toString());
                return dt.toInstant().getMillis() / 1000L;
            };
        } else if (toFormat.equals("ISO") && fromFormat.startsWith("pattern:")) {
            fromFormat = fromFormat.split("pattern:")[1].trim();
            convertTime = (k) -> {
                DateTimeFormatter fmtPattern = DateTimeFormat.forPattern(fromFormat);
                DateTime dt = fmtPattern.parseDateTime(k.toString());
                return dt.toDateTimeISO().toString();
            };
        }
    }


    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {

        if (value != null) {

            Map<String, Object> newEvent = new HashMap<>();
            newEvent.putAll(value);

            if (forceOutputString) {
                newEvent.put(dimensionToProcess, convertTime.apply(newEvent.get(dimensionToProcess)).toString());
            } else {
                newEvent.put(dimensionToProcess, convertTime.apply(newEvent.get(dimensionToProcess)));
            }
            return new KeyValue<>(key, newEvent);
        } else {
            return new KeyValue<>(key, null);
        }
    }


    @Override
    public void stop() {

    }
}
