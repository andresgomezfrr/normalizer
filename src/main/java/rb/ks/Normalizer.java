package rb.ks;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rb.ks.exceptions.PlanBuilderException;
import rb.ks.model.PlanModel;
import rb.ks.serializers.JsonSerde;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class Normalizer {
    private static final Logger log = LoggerFactory.getLogger(Normalizer.class);

    public static void main(String[] args) throws IOException, PlanBuilderException {
        if(args.length == 5) {
            File file = new File(args[0]);

            Properties streamsConfiguration = new Properties();
            streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, args[1]);
            streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, args[2]);
            streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, args[3]);
            streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, args[4]);
            streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());

            ObjectMapper objectMapper = new ObjectMapper();
            PlanModel model = objectMapper.readValue(file, PlanModel.class);
            log.info("Execution plan: {}", model.printExecutionPlan());
            log.info("-------- TOPOLOGY BUILD START --------");
            StreamBuilder streamBuilder = new StreamBuilder(args[3]);
            KStreamBuilder builder = streamBuilder.builder(model);
            log.info("--------  TOPOLOGY BUILD END  --------");

            KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
            streams.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                streamBuilder.close();
                streams.close();
                log.info("Stopped Normalizer instance.");
            }));


            log.info("Started Normalizer with conf {}", streamsConfiguration);
        } else {
            log.error("Execute: java -cp ${JAR_PATH} rb.ks.Normalizer <config_file> <zookeeper_connect> <bootstrap_servers> <application_id> <threads>");
        }
    }
}
