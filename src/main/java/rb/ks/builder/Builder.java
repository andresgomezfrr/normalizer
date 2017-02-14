package rb.ks.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsException;
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
    Config config;
    StreamBuilder streamBuilder;
    KafkaStreams streams;
    ThreadBootstraper threadBootstraper;

    public Builder(Config config) throws Exception {
        this.config = config;

        streamBuilder = new StreamBuilder(config.get(APPLICATION_ID_CONFIG));

        Class bootstraperClass = Class.forName(config.get(BOOTSTRAPER_CLASSNAME));
        threadBootstraper = (ThreadBootstraper) bootstraperClass.newInstance();
        threadBootstraper.init(this, config);
        threadBootstraper.start();
    }

    public void updateStreamConfig(String streamConfig) throws IOException, PlanBuilderException {
        if (streams != null) {
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

        streams = new KafkaStreams(builder, config.getProperties());
        streams.setUncaughtExceptionHandler((thread, exception) -> {
            if(!exception.getMessage().contains("Topic not found")) {
                log.error(exception.getMessage(), exception);
            } else {
                log.warn("Creating topics, try execute Normalizer again!");
            }

            threadBootstraper.interrupt();
            streamBuilder.close();
        });
        streams.start();

        log.info("Started Normalizer with conf {}", config.getProperties());
    }

    public void close() {
        threadBootstraper.interrupt();
        streamBuilder.close();
        if (streams != null) streams.close();
    }
}
