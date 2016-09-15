package rb.ks.funcs;

import org.apache.kafka.streams.KeyValue;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Seconds;
import rb.ks.constants.Dimension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SplitterFlatMapper extends FlatMapperFunction {

    SplitterModel splitter;

    @Override
    public void prepare(Map<String, Object> properties) {
        Map<String, Object> splitterMap = (Map<String, Object>) properties.get("splitter");

        splitter = new SplitterModel((List<String>) splitterMap.get("dimensions"), "60");
    }

    @Override
    public Iterable<KeyValue<String, Map<String, Object>>> process(String key, Map<String, Object> value) {
        List<KeyValue<String, Map<String, Object>>> generatedPackets = new ArrayList<>();

        if (value.containsKey(Dimension.FIRST_SWITCHED) && value.containsKey(Dimension.TIMESTAMP)) {

            DateTime packet_start = new DateTime(Long.parseLong(value.get(Dimension.FIRST_SWITCHED).toString()) * 1000, DateTimeZone.UTC);
            DateTime packet_end = new DateTime(Long.parseLong(value.get(Dimension.TIMESTAMP).toString()) * 1000, DateTimeZone.UTC);

            DateTime this_start;
            DateTime this_end = packet_start;

            long totalDiff = Seconds.secondsBetween(packet_start, packet_end).getSeconds();

            Map<String, Long> data_map = new HashMap<>();
            Map<String, Long> data_count = new HashMap<>();
            for (String dimension : splitter.dimensions) {

                try {
                    if (value.containsKey(dimension)) {
                        data_map.put(dimension, Long.parseLong(value.get(dimension).toString()));
                        data_count.put(dimension, 0L);
                    }
                } catch (NumberFormatException e) {
                    // TODO add log warning
                    return generatedPackets;
                }

            }


            do {
                long diff, this_data;

                this_start = this_end;
                this_end = this_start.plusSeconds(splitter.interval - this_start.getSecondOfMinute());
                if (this_end.isAfter(packet_end)) this_end = packet_end;
                diff = Seconds.secondsBetween(this_start, this_end).getSeconds();

                Map<String, Object> to_send = new HashMap<>();
                to_send.putAll(value);
                to_send.put(Dimension.TIMESTAMP, this_start.getMillis() / 1000);
                to_send.remove(Dimension.FIRST_SWITCHED);

                for (Map.Entry<String, Long> entry : data_map.entrySet()) {

                    this_data = 0;

                    if (totalDiff == 0)
                        this_data = entry.getValue();
                    else
                        this_data = (long) Math.ceil(entry.getValue() * diff / totalDiff);

                    Long data_dimension = data_count.get(entry.getKey());
                    data_dimension += this_data;
                    data_count.put(entry.getKey(), data_dimension);

                    to_send.put(entry.getKey(), this_data);
                }

                generatedPackets.add(new KeyValue<>(key, to_send));

            } while (this_end.isBefore(packet_end));

            for (Map.Entry<String, Long> data : data_map.entrySet()) {
                if (data.getValue() != data_count.get(data.getKey())) {
                    int last_index = generatedPackets.size() - 1;
                    KeyValue<String, Map<String, Object>> last = generatedPackets.get(last_index);
                    long new_data = ((long) last.value.get(data.getKey()) + (data.getValue() - data_count.get(data.getKey())));

                    if (new_data > 0) last.value.put(data.getKey(), new_data);
                }
            }

        } else if (value.containsKey(Dimension.TIMESTAMP)) {
            try {
                for (String dimension : splitter.dimensions) {
                    if (value.containsKey(dimension)) {
                        Long data = Long.parseLong(value.get(dimension).toString());
                        value.put(dimension, data);
                    }/* else {
                        // TODO add log warn
                        return generatedPackets;
                    }*/
                }

                generatedPackets.add(new KeyValue<>(key, value));

            } catch (NumberFormatException e) {
                // TODO add log warn
                return generatedPackets;
            }
        } else {

            try {
                for (String dimension : splitter.dimensions) {
                    if (value.containsKey(dimension)) {
                        Long data = Long.parseLong(value.get(dimension).toString());
                        value.put(dimension, data);
                        // TODO add log warn
                    }/* else {
                        // TODO add log warn
                        return generatedPackets;
                    }*/
                }
                generatedPackets.add(new KeyValue<>(key, value));
            } catch (NumberFormatException e) {
                // TODO add log warn
                return generatedPackets;
            }
        }


        return generatedPackets;
    }

    @Override
    public void stop() {

    }

    @Override
    public String toString() {

        StringBuilder builder = new StringBuilder();

        builder.append("[");
        builder.append(splitter.toString());
        builder.append("]");

        return builder.toString();
    }

    public class SplitterModel {
        List<String> dimensions;
        Integer interval;
        Long first_switch;
        Long timestamp;

        SplitterModel(List<String> dimensions, String interval) {
            this.dimensions = dimensions;
            this.first_switch = first_switch;
            this.timestamp = timestamp;
            this.interval = interval != null ? Integer.valueOf(interval) : 60;
        }

        public List<String> getDimensions() {
            return dimensions;
        }

        public Integer getInterval() {
            return interval;
        }

        public Long getFirstSwitch() {
            return first_switch;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(" {")
                    .append("dimensions: ").append(dimensions).append(", ")
                    .append("interval: ").append(interval)
                    .append("} ");

            return builder.toString();
        }
    }

}
