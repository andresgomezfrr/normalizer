package rb.ks.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rb.ks.builder.bootstrap.KafkaBootstraper;
import rb.ks.exceptions.PlanBuilderException;
import rb.ks.model.PlanModel;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;

public class Builder {
    private static final Logger log = LoggerFactory.getLogger(Builder.class);
    Properties properties = new Properties();
    StreamBuilder streamBuilder;
    KafkaStreams streams;
    KafkaBootstraper kafkaBootstraper;

    public Builder(Map<String, String> properties) throws IOException, PlanBuilderException {
        this.properties.putAll(properties);
        streamBuilder = new StreamBuilder(properties.get(APPLICATION_ID_CONFIG));
        kafkaBootstraper = new KafkaBootstraper();
        kafkaBootstraper.init(this, properties);
        kafkaBootstraper.start();
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
        kafkaBootstraper.close();
        streamBuilder.close();
        if(streams != null) streams.close();
    }
}
