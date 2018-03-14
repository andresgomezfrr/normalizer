package io.wizzie.ks.normalizer.funcs.impl;

import io.wizzie.ks.normalizer.funcs.MapperFunction;
import io.wizzie.ks.normalizer.metrics.MetricsManager;
import io.wizzie.ks.normalizer.utils.ConvertFrom;
import org.apache.kafka.streams.KeyValue;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class FieldTypeConverterMapper extends MapperFunction {

    List<Map<String, Object>> conversions;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        conversions = (List<Map<String, Object>>) properties.get("conversions");
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> message) {

        for (Map<String, Object> conversion : conversions) {
            String dimension = checkNotNull((String) conversion.get("dimension"), "dimension cannot be null!");
            String newDimension = (String) conversion.get("newDimension");

            // Get value
            Object value = message.get(dimension);

            // Check that value is not null
            if (value != null) {
                // Source
                String from = checkNotNull((String) conversion.get("from"), "from cannot be null!");
                // Destiny
                String to = checkNotNull((String) conversion.get("to"), "to cannot be null!");
                // Converted value
                Object convertedValue = null;

                // Performance conversion
                switch (ConvertFrom.valueOf(to.toUpperCase())) {
                    case NUMBER:
                        try {
                            convertedValue = ConvertFrom.valueOf(from.toUpperCase()).toNumber(value);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        break;
                    case STRING:
                        convertedValue = ConvertFrom.valueOf(from.toUpperCase()).toString(value);
                        break;
                    case BOOLEAN:
                        convertedValue = ConvertFrom.valueOf(from.toUpperCase()).toBoolean(value);
                        break;
                }

                // Add converted value to map
                if (newDimension != null)
                    message.put(newDimension, convertedValue);
                else
                    message.put(dimension, convertedValue);
            }
        }

        return new KeyValue<>(key, message);
    }

    @Override
    public void stop() {
    }
}
