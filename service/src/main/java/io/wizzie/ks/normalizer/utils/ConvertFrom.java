package io.wizzie.ks.normalizer.utils;

import io.wizzie.ks.normalizer.funcs.impl.FieldTypeConverterMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;

public enum ConvertFrom {

    NUMBER {
        public Number toNumber(Object value) {
            if (check(value))
                return (Number) value;
            else {
                log.debug("Value '" + value + "' is not Number type!");
                return null;
            }
        }

        public String toString(Object value) {
            if (check(value))
                return String.valueOf(value);
            else {
                log.debug("Value '" + value + "' is not Number type!");
                return null;
            }
        }

        public Boolean toBoolean(Object value) {
            if (check(value))
                return ((Number) value).longValue() > 0;
            else {
                log.debug("Value '" + value + "' is not Number type!");
                return null;
            }

        }

        Boolean check(Object value) {
            return value instanceof Number;
        }
    },

    STRING {
        public Number toNumber(Object value) throws ParseException {
            if (check(value))
                return NumberFormat.getInstance(Locale.US).parse((String) value);
            else {
                log.debug("Value '" + value + "' is not String type!");
                return null;
            }
        }

        public String toString(Object value) {
            if (check(value))
                return (String) value;
            else {
                log.debug("Value '" + value + "' is not String type!");
                return null;
            }
        }

        public Boolean toBoolean(Object value) {
            if (check(value))
                return Boolean.valueOf((String) value);
            else {
                log.debug("Value '" + value + "' is not String type!");
                return null;
            }
        }

        Boolean check(Object value) {
            return value instanceof String;
        }
    },

    BOOLEAN {
        public Number toNumber(Object value) throws ParseException {
            if (check(value))
                return (Boolean) value ? 1 : 0;
            else {
                log.debug("Value '" + value + "' is not Boolean type!");
                return null;
            }
        }

        public String toString(Object value) {
            if (check(value))
                return (Boolean) value ? "true" : "false";
            else {
                log.debug("Value '" + value + "' is not Boolean type!");
                return null;
            }
        }

        public Boolean toBoolean(Object value) {
            if (check(value))
                return (Boolean) value;
            else {
                log.debug("Value '" + value + "' is not Boolean type!");
                return null;
            }
        }

        Boolean check(Object value) {
            return value instanceof Boolean;
        }
    };
    private static final Logger log = LoggerFactory.getLogger(FieldTypeConverterMapper.class);

    public Number toNumber(Object value) throws ParseException {
        throw new AbstractMethodError();
    }

    public String toString(Object value) {
        throw new AbstractMethodError();
    }

    public Boolean toBoolean(Object value) {
        throw new AbstractMethodError();
    }

}
