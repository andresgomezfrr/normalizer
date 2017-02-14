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
import rb.ks.exceptions.TryToDoLoopException;
import rb.ks.funcs.*;
import rb.ks.model.*;
import rb.ks.serializers.JsonSerde;

import java.util.*;

public class StreamBuilder {
    private static final Logger log = LoggerFactory.getLogger(StreamBuilder.class);

    private Map<String, KStream<String, Map<String, Object>>> kStreams = new HashMap<>();
    private Map<String, Map<String, Function>> streamFunctions = new HashMap<>();
    private Set<String> usedStores = new HashSet<>();
    private Set<String> addedFuncsToStreams = new HashSet<>();
    private Set<String> addedSinksToStreams = new HashSet<>();
    private Boolean addedNewStream = true;

    public KStreamBuilder builder(PlanModel model) throws PlanBuilderException, TryToDoLoopException {
        model.validate();

        clean();

        KStreamBuilder builder = new KStreamBuilder();
        createInputStreams(builder, model);

        for(int iteration = 0; addedNewStream; iteration++) {
            log.info("*** Iteration [{}]", iteration);
            addedNewStream = false;
            addFuncsToStreams(builder, model);
            addSinksToStreams(model);
        }

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

    private void createInputStreams(KStreamBuilder builder, PlanModel model) {
        for (Map.Entry<String, List<String>> inputs : model.getInputs().entrySet()) {
            String topic = inputs.getKey();
            KStream<String, Map<String, Object>> kstream = builder.stream(topic);
            for (String stream : inputs.getValue()) {
                log.info("Creating stream [{}]", stream);
                kStreams.put(stream, kstream);
            }
        }
    }

    private void addFuncsToStreams(KStreamBuilder builder, PlanModel model) {
        for (Map.Entry<String, StreamModel> streams : model.getStreams().entrySet()) {
            if (!addedFuncsToStreams.contains(streams.getKey()) && kStreams.containsKey(streams.getKey())) {
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
                            log.info("Add function [{}] to stream [{}]", name, streams.getKey());
                            Function func = makeFunction(className, properties);
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
                            log.error("Couldn't find the class associated with the function {}", className);
                        } catch (InstantiationException | IllegalAccessException e) {
                            log.error("Couldn't create the instance associated with the function " + className, e);
                        }
                    }
                }

                addedFuncsToStreams.add(streams.getKey());
            } else {
                if (!kStreams.containsKey(streams.getKey())) {
                    log.debug("Stream {} is to later iteration.", streams.getKey());
                }
            }
        }
    }

    private void addSinksToStreams(PlanModel model) throws TryToDoLoopException {
        List<String> generatedStreams = new ArrayList<>();
        for (Map.Entry<String, StreamModel> streams : model.getStreams().entrySet()) {
            if (!addedSinksToStreams.contains(streams.getKey()) && !generatedStreams.contains(streams.getKey())) {
                List<SinkModel> sinks = streams.getValue().getSinks();
                for (SinkModel sink : sinks) {
                    KStream<String, Map<String, Object>> kStream = kStreams.get(streams.getKey());
                    if (kStream != null) {
                        log.info("Send to {} [{}]", sink.getType(), sink.getTopic());

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

                        FunctionModel filterModel = sink.getFilter();
                        if (filterModel != null) {
                            String className = filterModel.getClassName();
                            try {
                                FilterFunc filter = (FilterFunc) makeFunction(className, filterModel.getProperties());
                                kStream = kStream.filter(filter);
                            } catch (ClassNotFoundException e) {
                                log.error("Couldn't find the class associated with the function {}", className);
                            } catch (InstantiationException | IllegalAccessException e) {
                                log.error("Couldn't create the instance associated with the function " + className, e);
                            }
                        }

                        if (sink.getType().equals(SinkModel.KAFKA_TYPE)) {
                            kStream.to(
                                    (key, value, numPartitions) ->
                                            Utils.abs(Utils.murmur2(key.getBytes())) % numPartitions, sink.getTopic()

                            );
                        } else if (sink.getType().equals(SinkModel.STREAM_TYPE)) {
                            String newStreamName = sink.getTopic();
                            if(!kStreams.containsKey(newStreamName)) {
                                addedNewStream = true;
                                KStream<String, Map<String, Object>> newBranch = kStream.branch((key, value) -> true)[0];
                                kStreams.put(newStreamName, newBranch);
                                log.info("Creating stream [{}]", sink.getTopic());
                                generatedStreams.add(sink.getTopic());
                            } else {
                                throw new TryToDoLoopException("Loop from [" + streams.getKey() + "] to [" + newStreamName + "]");
                            }
                        }
                    }
                }
                addedSinksToStreams.add(streams.getKey());
            }
        }
        generatedStreams.clear();
    }

    private void clean() {
        addedSinksToStreams.clear();
        streamFunctions.clear();
        addedFuncsToStreams.clear();
        usedStores.clear();
        kStreams.clear();
        addedNewStream = true;
    }

    private Function makeFunction(String className, Map<String, Object> properties)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        Class funcClass = Class.forName(className);
        Function func = (Function) funcClass.newInstance();
        func.init(properties);
        return func;
    }
}
