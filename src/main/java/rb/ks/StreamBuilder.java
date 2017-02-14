package rb.ks;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import rb.ks.exceptions.PlanBuilderException;
import rb.ks.funcs.FlatMapperFunction;
import rb.ks.funcs.Function;
import rb.ks.funcs.MapperFunction;
import rb.ks.funcs.MapperStoreFunction;
import rb.ks.model.*;
import rb.ks.serializers.JsonSerde;

import java.util.*;

public class StreamBuilder {
    Map<String, KStream<String, Map<String, Object>>> kStreams = new HashMap<>();
    Map<String, Map<String, Function>> streamFunctions = new HashMap<>();
    Set<String> usedStores = new HashSet<>();

    public KStreamBuilder builder(PlanModel model) throws PlanBuilderException {
        // Validate the model
        model.validate();

        // Cleaning last build
        clean();

        //Create new build
        KStreamBuilder builder = new KStreamBuilder();
        createInputs(builder, model);
        createFuncs(builder, model);
        addTimestampter(model);
        addSinks(model);

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

    public void close(){
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

                    String name = funcModel.getName();
                    String className = funcModel.getClassName();
                    Map<String, Object> properties = funcModel.getProperties();
                    List<String> stores = funcModel.getStores();
                    if (stores != null) {
                        properties.put("__STORES", stores);
                        stores.forEach(store -> {
                            if (!usedStores.contains(store)) {
                                System.out.println("Creating store: " + store);
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
                }
            }
        }
    }

    private void addTimestampter(PlanModel model) {
        for (Map.Entry<String, StreamModel> streams : model.getStreams().entrySet()) {
            KStream<String, Map<String, Object>> kStream = kStreams.get(streams.getKey());

            TimestamperModel timestamperModel = streams.getValue().getTimestamper();

            if (timestamperModel != null) {
                // Transform timestamp
                KStream<String, Map<String, Object>> kStreamTimestampered = kStream.mapValues((value) -> {
                    String timestampDim = timestamperModel.getTimestampDim();

                    Map<String, Object> newEvent = new HashMap<>(value);
                    newEvent.put(timestampDim, timestamperModel.generateTimestamp(value.get(timestampDim)));
                    return newEvent;
                });

                kStreams.put(streams.getKey(), kStreamTimestampered);
            }
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
                            (key, value) -> {
                                Object newKey = value.get(sink.getPartitionBy());
                                if (newKey != null)
                                    return new KeyValue<>(newKey.toString(), value);
                                else {
                                    // TODO: Logger the new partition key is not valid!
                                    return new KeyValue<>(key, value);
                                }
                            }
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
