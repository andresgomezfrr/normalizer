package zz.ks;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import zz.ks.exceptions.PlanBuilderException;
import zz.ks.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamBuilder {
    Map<String, KStream<String, Map<String, Object>>> kStreams = new HashMap<>();

    public KStreamBuilder builder(PlanModel model) throws PlanBuilderException {
        // Validate the model
        model.validate();

        // Cleaning last build
        clean();

        //Create new build
        KStreamBuilder builder = new KStreamBuilder();
        createInputs(builder, model);
        addMappers(model);
        addTimestampter(model);
        addSinks(model);

        return builder;
    }

    public KStream<String, Map<String, Object>> getKStream(String streamName) {
        return kStreams.get(streamName);
    }

    private void createInputs(KStreamBuilder builder, PlanModel model) {
        for (Map.Entry<String, List<String>> inputs : model.getInputs().entrySet()) {
            String topic = inputs.getKey();
            KStream<String, Map<String, Object>> kstream = builder.stream(topic);
            for (String stream : inputs.getValue()) kStreams.put(stream, kstream);
        }
    }

    private void addMappers(PlanModel model) {
        for (Map.Entry<String, StreamModel> streams : model.getStreams().entrySet()) {
            KStream<String, Map<String, Object>> kStream = kStreams.get(streams.getKey());

            KStream<String, Map<String, Object>> mapKStream = kStream.map((key, value) -> {
                Map<String, Object> newEvent = new HashMap<>();

                for (MapperModel mapper : streams.getValue().getMappers()) {
                    Integer depth = mapper.dimPath.size() - 1;

                    Map<String, Object> levelPath = new HashMap<>(value);
                    for (Integer level = 0; level < depth; level++) {
                        if (levelPath != null) {
                            levelPath = (Map<String, Object>) levelPath.get(mapper.dimPath.get(level));
                        }
                    }

                    if (levelPath != null) newEvent.put(mapper.as, levelPath.get(mapper.dimPath.get(depth)));
                }

                return new KeyValue<>(key, newEvent);
            });

            kStreams.put(streams.getKey(), mapKStream);
        }
    }

    private void addTimestampter(PlanModel model) {
        for (Map.Entry<String, StreamModel> streams : model.getStreams().entrySet()) {
            KStream<String, Map<String, Object>> kStream = kStreams.get(streams.getKey());

            // Transform timestamp
            kStream = kStream.map((key, value) -> {
                TimestamperModel timestamperModel = streams.getValue().getTimestamper();
                String timestampDim = timestamperModel.getTimestampDim();

                Map<String, Object> newEvent = new HashMap<>(value);
                newEvent.put(timestampDim, timestamperModel.generateTimestamp(value.get(timestampDim)));
                return new KeyValue<>(key, newEvent);
            });

            kStreams.put(streams.getKey(), kStream);
        }
    }

    private void addSinks(PlanModel model) {
        for (Map.Entry<String, StreamModel> streams : model.getStreams().entrySet()) {
            List<SinkModel> sinks = streams.getValue().getSinks();

            for (SinkModel sink : sinks) {
                KStream<String, Map<String, Object>> kStream = kStreams.get(streams.getKey());

                // Repartition stream before send it
                if (!sink.getPartitionBy().equals(SinkModel.PARTITION_BY_KEY)) {
                    kStream = kStream.map(
                            (key, value) ->
                                    new KeyValue<>((String) value.get(sink.getPartitionBy()), value)
                    );
                }

                // Send stream
                kStream.to(
                        (key, value, numPartitions) ->
                                Utils.abs(Utils.murmur2(key.getBytes())) % numPartitions, sink.getTopic()
                );
            }
        }
    }

    private void clean() {
        kStreams.clear();
    }
}
