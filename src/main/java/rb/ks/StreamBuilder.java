package rb.ks;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rb.ks.exceptions.PlanBuilderException;
import rb.ks.funcs.*;
import rb.ks.model.*;
import rb.ks.serializers.JsonSerde;

import java.util.*;

public class StreamBuilder {
    private static final Logger log = LoggerFactory.getLogger(StreamBuilder.class);

    Map<String, KStream<String, Map<String, Object>>> kStreams = new HashMap<>();
    Map<String, Map<String, Function>> streamFunctions = new HashMap<>();
    Set<String> usedStores = new HashSet<>();
    List<String> processedFuncs = new ArrayList<>();
    List<String> processedTimestamper = new ArrayList<>();
    List<String> processedSinks = new ArrayList<>();
    Boolean newStream = false;

    public KStreamBuilder builder(PlanModel model) throws PlanBuilderException {
        model.validate();

        clean();

        KStreamBuilder builder = new KStreamBuilder();
        createInputs(builder, model);

        do {
            newStream = false;
            createFuncs(builder, model);
            addTimestampter(model);
            addSinks(model);
        } while (newStream);

        return builder;
    }

    public KStream<String, Map<String, Object>> getKStream(String streamName) {
        return kStreams.get(streamName);
    }

    public Map<String, Function> getFunctions(String streamName) {
        return streamFunctions.get(streamName);
    }

    public Set<String> usedStores() {
        return usedStores;
    }

    public void close() {
        streamFunctions.forEach((stream, functions) -> functions.forEach((name, fucntion) -> fucntion.stop()));
        clean();
    }

    private void createInputs(KStreamBuilder builder, PlanModel model) {
        for (Map.Entry<String, List<String>> inputs : model.getInputs().entrySet()) {
            String topic = inputs.getKey();
            KStream<String, Map<String, Object>> kstream = builder.stream(topic);
            for (String stream : inputs.getValue()) kStreams.put(stream, kstream);
        }
    }

    private void createFuncs(KStreamBuilder builder, PlanModel model) {
        for (Map.Entry<String, StreamModel> streams : model.getStreams().entrySet()) {
            List<FunctionModel> funcModels = streams.getValue().getFuncs();

            if (funcModels != null) {
                for (FunctionModel funcModel : funcModels) {
                    KStream<String, Map<String, Object>> kStream = kStreams.get(streams.getKey());
                    if (kStream != null && !processedFuncs.contains(streams.getKey())) {
                        String name = funcModel.getName();
                        String className = funcModel.getClassName();
                        Map<String, Object> properties = funcModel.getProperties();
                        List<String> stores = funcModel.getStores();

                        if (stores != null) {
                            properties.put("__STORES", stores);
                            stores.forEach(store -> {
                                if (!usedStores.contains(store)) {
                                    StateStoreSupplier storeSupplier = Stores.create(store)
                                            .withKeys(Serdes.String())
                                            .withValues(new JsonSerde())
                                            .persistent()
                                            .build();

                                    builder.addStateStore(storeSupplier);
                                    usedStores.add(store);
                                }
                            });
                        }

                        try {
                            Class funcClass = Class.forName(className);
                            Function func = (Function) funcClass.newInstance();
                            func.init(properties);

                            if (func instanceof MapperFunction) {
                                kStream = kStream.map((MapperFunction) func);
                            } else if (func instanceof FlatMapperFunction) {
                                kStream = kStream.flatMap((FlatMapperFunction) func);
                            } else if (func instanceof MapperStoreFunction) {
                                kStream = kStream.transform(() ->
                                        (MapperStoreFunction) func, stores.toArray(new String[stores.size()])
                                );
                            } else if (func instanceof FilterFunc) {
                                kStream = kStream.filter((FilterFunc) func);
                            }

                            Map<String, Function> functions = streamFunctions.get(streams.getKey());
                            if (functions == null) functions = new HashMap<>();
                            functions.put(name, func);

                            streamFunctions.put(streams.getKey(), functions);
                            kStreams.put(streams.getKey(), kStream);
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        } catch (InstantiationException | IllegalAccessException e) {
                            e.printStackTrace();
                        }

                        processedFuncs.add(streams.getKey());
                    } else {
                        if (kStream == null) log.info("Stream {} is to the next iteration.", streams.getKey());
                    }
                }
            }
        }
    }

    private void addTimestampter(PlanModel model) {
        for (Map.Entry<String, StreamModel> streams : model.getStreams().entrySet()) {
            KStream<String, Map<String, Object>> kStream = kStreams.get(streams.getKey());
            if (kStream != null && !processedTimestamper.contains(streams.getKey())) {

                TimestamperModel timestamperModel = streams.getValue().getTimestamper();

                if (timestamperModel != null) {
                    KStream<String, Map<String, Object>> kStreamTimestampered = kStream.mapValues((value) -> {
                        String timestampDim = timestamperModel.getTimestampDim();
                        Map<String, Object> newEvent = new HashMap<>(value);
                        newEvent.put(timestampDim, timestamperModel.generateTimestamp(value.get(timestampDim)));
                        return newEvent;
                    });

                    kStreams.put(streams.getKey(), kStreamTimestampered);
                    processedTimestamper.add(streams.getKey());
                }
            }
        }
    }

    private void addSinks(PlanModel model) {
        List<String> generatedStreams = new ArrayList<>();
        for (Map.Entry<String, StreamModel> streams : model.getStreams().entrySet()) {
            if (!processedSinks.contains(streams.getKey()) && !generatedStreams.contains(streams.getKey())) {
                List<SinkModel> sinks = streams.getValue().getSinks();
                for (SinkModel sink : sinks) {
                    KStream<String, Map<String, Object>> kStream = kStreams.get(streams.getKey());
                    if (kStream != null) {
                        if (!sink.getPartitionBy().equals(SinkModel.PARTITION_BY_KEY)) {
                            kStream = kStream.map(
                                    (key, value) -> {
                                        Object newKey = value.get(sink.getPartitionBy());
                                        if (newKey != null)
                                            return new KeyValue<>(newKey.toString(), value);
                                        else {
                                            log.warn("Partition key {} isn't on message {}",
                                                    sink.getPartitionBy(), value);
                                            return new KeyValue<>(key, value);
                                        }
                                    }
                            );
                        }

                        if (sink.getType().equals(SinkModel.KAFKA_TYPE)) {
                            kStream.to(
                                    (key, value, numPartitions) ->
                                            Utils.abs(Utils.murmur2(key.getBytes())) % numPartitions, sink.getTopic()

                            );
                        } else if (sink.getType().equals(SinkModel.STREAM_TYPE)) {
                            newStream = true;
                            KStream<String, Map<String, Object>> newBranch = kStream.branch((key, value) -> true)[0];
                            kStreams.put(sink.getTopic(), newBranch);
                            generatedStreams.add(sink.getTopic());
                        }
                    }
                }
                processedSinks.add(streams.getKey());
            }
        }
        generatedStreams.clear();
    }

    private void clean() {
        kStreams.clear();
    }
}
