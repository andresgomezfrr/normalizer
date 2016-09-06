package zz.ks;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.StreamPartitioner;
import zz.ks.exceptions.PlanBuilderException;
import zz.ks.model.MapperModel;
import zz.ks.model.PlanModel;
import zz.ks.model.SinkModel;
import zz.ks.model.StreamModel;

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

            mapKStream.print();
            kStreams.put(streams.getKey(), mapKStream);
        }
    }

    private void addSinks(PlanModel model) {
        for (Map.Entry<String, StreamModel> streams : model.getStreams().entrySet()) {
            List<SinkModel> sinks = streams.getValue().getSinks();
            KStream<String, Map<String, Object>> kStream = kStreams.get(streams.getKey());

            for (SinkModel sink : sinks) {
                kStream.to(
                        (key, value, numPartitions) -> {
                            if (sink.getPartitionBy().equals(SinkModel.PARTITION_BY_KEY)) {
                                return Utils.abs(Utils.murmur2(key.getBytes())) % numPartitions;
                            } else {
                                return Utils.abs(Utils.murmur2(((String) value.get(sink.getPartitionBy())).getBytes())) % numPartitions;
                            }
                        }, sink.getTopic());
            }
        }
    }

    private void clean() {
        kStreams.clear();
    }
}
