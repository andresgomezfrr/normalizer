package rb.ks.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rb.ks.builder.bootstrap.ThreadBootstraper;
import rb.ks.builder.config.Config;
import rb.ks.exceptions.PlanBuilderException;
import rb.ks.model.PlanModel;
import rb.ks.serializers.JsonSerde;

import java.io.IOException;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.*;
import static rb.ks.builder.config.Config.ConfigProperties.BOOTSTRAPER_CLASSNAME;

public class Builder {
    private static final Logger log = LoggerFactory.getLogger(Builder.class);
    Properties properties = new Properties();
    StreamBuilder streamBuilder;
    KafkaStreams streams;
    ThreadBootstraper threadBootstraper;

    public Builder(Config config) throws Exception {
        properties.put(ZOOKEEPER_CONNECT_CONFIG, config.get(ZOOKEEPER_CONNECT_CONFIG));
        properties.put(BOOTSTRAP_SERVERS_CONFIG, config.get(BOOTSTRAP_SERVERS_CONFIG));
        properties.put(APPLICATION_ID_CONFIG, config.get(APPLICATION_ID_CONFIG));
        properties.put(NUM_STREAM_THREADS_CONFIG, config.get(NUM_STREAM_THREADS_CONFIG).toString());
        properties.put(KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());

        streamBuilder = new StreamBuilder(properties.getProperty(APPLICATION_ID_CONFIG));

        Class bootstraperClass = Class.forName(config.get(BOOTSTRAPER_CLASSNAME));
        threadBootstraper = (ThreadBootstraper) bootstraperClass.newInstance();
        threadBootstraper.init(this, config);
        threadBootstraper.start();
    }

    public void updateStreamConfig(String streamConfig) throws IOException, PlanBuilderException {
        if(streams != null) {
            streamBuilder.close();
            streams.close();
            log.info("Stopped Normalizer process.");
        }

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(streamConfig, PlanModel.class);
        log.info("Execution plan: {}", model.printExecutionPlan());
        log.info("-------- TOPOLOGY BUILD START --------");
        KStreamBuilder builder = streamBuilder.builder(model);
        log.info("--------  TOPOLOGY BUILD END  --------");

        streams = new KafkaStreams(builder, properties);
        streams.start();
        log.info("Started Normalizer with conf {}", properties);
    }

    public void close(){
        threadBootstraper.interrupt();
        streamBuilder.close();
        if(streams != null) streams.close();
    }
}
