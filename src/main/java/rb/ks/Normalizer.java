package rb.ks;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rb.ks.builder.Builder;
import rb.ks.exceptions.PlanBuilderException;
import rb.ks.serializers.JsonSerde;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Normalizer {
    private static final Logger log = LoggerFactory.getLogger(Normalizer.class);

    public static void main(String[] args) throws IOException, PlanBuilderException {
        if (args.length == 5) {
            File file = new File(args[0]);

            Map<String, String> configuration = new HashMap<>();
            configuration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, args[1]);
            configuration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, args[2]);
            configuration.put(StreamsConfig.APPLICATION_ID_CONFIG, args[3]);
            configuration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, args[4]);
            configuration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            configuration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());

            Builder builder = new Builder(configuration);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                builder.close();
                log.info("Stopped Normalizer process.");
            }));


        } else {
            log.error("Execute: java -cp ${JAR_PATH} rb.ks.Normalizer <config_file> <zookeeper_connect> <bootstrap_servers> <application_id> <threads>");
        }
    }
}
