package io.wizzie.ks.normalizer.utils;

import io.wizzie.ks.normalizer.exceptions.ConverterTypeException;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;

public enum ConvertFrom {

    NUMBER {
        public Number toNumber(Object value) {
            if(check(value))
                return (Number) value;
            else
                throw new ConverterTypeException("Value '" + value + "' is not Number type!");
        }

        public String toString(Object value) {
            if(check(value))
                return String.valueOf(value);
            else
                throw new ConverterTypeException("Value '" + value + "' is not Number type!");
        }

        public Boolean toBoolean(Object value) {
           if(check(value))
               return ((Number) value).longValue() > 0;
           else
               throw new ConverterTypeException("Value '" + value + "' is not Number type!");
        }

        Boolean check(Object value) { return value instanceof Number; }
    },

    STRING {
        public Number toNumber(Object value) throws ParseException {
            if(check(value))
                return NumberFormat.getInstance(Locale.US).parse((String) value) ;
            else
                throw new ConverterTypeException("Value '" + value + "' is not String type!");
        }

        public String toString(Object value) {
            if(check(value))
                return (String) value;
            else
                throw new ConverterTypeException("Value '" + value + "' is not String type!");
        }

        public Boolean toBoolean(Object value) {
            if(check(value))
                return Boolean.valueOf((String)value);
            else
                throw new ConverterTypeException("Value '" + value + "' is not String type!");
        }

        Boolean check(Object value) { return value instanceof String; }
    },

    BOOLEAN {
        public Number toNumber(Object value) throws ParseException {
            if(check(value))
                return (Boolean) value ? 1 : 0 ;
            else
                throw new ConverterTypeException("Value '" + value + "' is not Boolean type!");
        }

        public String toString(Object value) {
            if(check(value))
                return (Boolean) value ? "true" : "false";
            else
                throw new ConverterTypeException("Value '" + value + "' is not Boolean type!");
        }

        public Boolean toBoolean(Object value) {
            if(check(value))
                return (Boolean) value;
            else
                throw new ConverterTypeException("Value '" + value + "' is not Boolean type!");
        }

        Boolean check(Object value) {
            return value instanceof Boolean;
        }
    };

    public Number toNumber(Object value) throws ParseException { throw new AbstractMethodError(); }
    public String toString(Object value) { throw new AbstractMethodError(); }
    public Boolean toBoolean(Object value) { throw new AbstractMethodError(); }

}
