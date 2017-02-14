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
import rb.ks.metrics.MetricsManager;
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
    MetricsManager metricsManager;

    public Builder(Config config, MetricsManager metricsManager) throws Exception {
        this.config = config;
        this.metricsManager = metricsManager;

        streamBuilder = new StreamBuilder(config.get(APPLICATION_ID_CONFIG), metricsManager);

        Class bootstraperClass = Class.forName(config.get(BOOTSTRAPER_CLASSNAME));
        threadBootstraper = (ThreadBootstraper) bootstraperClass.newInstance();
        threadBootstraper.init(this, config.clone(), metricsManager);
        threadBootstraper.start();
    }

    public void updateStreamConfig(String streamConfig) throws IOException, PlanBuilderException {
        if(streams != null) {
            metricsManager.clean();
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
        streams.start();
        log.info("Started Normalizer with conf {}", config.getProperties());
    }

    public void close(){
        threadBootstraper.interrupt();
        streamBuilder.close();
        if(streams != null) streams.close();
    }
}
