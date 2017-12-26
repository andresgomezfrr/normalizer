package io.wizzie.ks.normalizer.builder;

import com.codahale.metrics.JmxAttributeGauge;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.*;
import io.wizzie.ks.normalizer.exceptions.PlanBuilderException;
import io.wizzie.ks.normalizer.model.PlanModel;
import io.wizzie.ks.normalizer.serializers.JsonSerde;
import io.wizzie.metrics.MetricsManager;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.IOException;

import static io.wizzie.ks.normalizer.base.builder.config.ConfigProperties.BOOTSTRAPER_CLASSNAME;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;


public class Builder implements Listener{
    private static final Logger log = LoggerFactory.getLogger(Builder.class);
    Config config;
    StreamBuilder streamBuilder;
    KafkaStreams streams;
    MetricsManager metricsManager;
    Bootstrapper bootstrapper;

    public Builder(Config config) throws Exception {
        this.config = config;

        config.put("key.serde", Serdes.StringSerde.class);
        config.put("value.serde", JsonSerde.class);

        metricsManager = new MetricsManager(config.getMapConf());
        registerKafkaMetrics(config, metricsManager);
        metricsManager.start();

        streamBuilder = new StreamBuilder(config.clone(), metricsManager);

        bootstrapper = BootstrapperBuilder.makeBuilder()
                .boostrapperClass(config.get(BOOTSTRAPER_CLASSNAME))
                .listener(this)
                .withConfigInstance(config)
                .build();
    }

    public void close() throws Exception {
        metricsManager.interrupt();
        streamBuilder.close();
        bootstrapper.close();
        if (streams != null) streams.close();
    }

    private void registerKafkaMetrics(Config config, MetricsManager metricsManager){
        Integer streamThreads = config.getOrDefault(NUM_STREAM_THREADS_CONFIG, 1);
        String appId = config.get(APPLICATION_ID_CONFIG);

        for (int i = 1; i <= streamThreads; i++) {
            try {

                // PRODUCER
                metricsManager.registerMetric("producer." + i + ".messages_send_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.producer:type=producer-metrics,client-id="
                                + appId + "-1-StreamThread-"
                                + i + "-producer"), "record-send-rate"));

                metricsManager.registerMetric("producer." + i + ".output_bytes_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.producer:type=producer-metrics,client-id="
                                + appId + "-1-StreamThread-"
                                + i + "-producer"), "outgoing-byte-rate"));

                metricsManager.registerMetric("producer." + i + ".incoming_bytes_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.producer:type=producer-metrics,client-id="
                                + appId + "-1-StreamThread-"
                                + i + "-producer"), "incoming-byte-rate"));

                // CONSUMER
                metricsManager.registerMetric("consumer." + i + ".max_lag",
                        new JmxAttributeGauge(new ObjectName("kafka.consumer:type=consumer-fetch-manager-metrics,client-id="
                                + appId + "-1-StreamThread-"
                                + i + "-consumer"), "records-lag-max"));

                metricsManager.registerMetric("consumer." + i + ".output_bytes_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.consumer:type=consumer-metrics,client-id="
                                + appId + "-1-StreamThread-"
                                + i + "-consumer"), "outgoing-byte-rate"));

                metricsManager.registerMetric("consumer." + i + ".incoming_bytes_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.consumer:type=consumer-metrics,client-id="
                                + appId + "-1-StreamThread-"
                                + i + "-consumer"), "incoming-byte-rate"));

                metricsManager.registerMetric("consumer." + i + ".records_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.consumer:type=consumer-fetch-manager-metrics,client-id="
                                + appId + "-1-StreamThread-"
                                + i + "-consumer"), "records-consumed-rate"));

            } catch (MalformedObjectNameException e) {
                log.warn("Metric not found");
            }
        }
    }

    @Override
    public void updateConfig(SourceSystem sourceSystem, String streamConfig) {
        if (streams != null) {
            metricsManager.clean();
            streamBuilder.close();
            streams.close();
            log.info("Clean Normalizer process");
        }

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            PlanModel model = objectMapper.readValue(streamConfig, PlanModel.class);
            log.info("Execution plan: {}", model.printExecutionPlan());
            log.info("-------- TOPOLOGY BUILD START --------");
            StreamsBuilder builder = streamBuilder.builder(model);
            log.info("--------  TOPOLOGY BUILD END  --------");

            Config configWithNewAppId = config.clone();
            String appId = configWithNewAppId.get(APPLICATION_ID_CONFIG);
            configWithNewAppId.put(APPLICATION_ID_CONFIG, String.format("%s_%s", appId, "normalizer"));
            streams = new KafkaStreams(builder.build(), configWithNewAppId.getProperties());

            streams.setUncaughtExceptionHandler((thread, exception) -> log.error(exception.getMessage(), exception));
            streams.start();

            log.info("Started Normalizer with conf {}", config.getProperties());
        } catch (PlanBuilderException | IOException e) {
            log.error(e.getMessage(), e);
        }
    }
}
