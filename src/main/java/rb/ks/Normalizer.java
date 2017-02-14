package rb.ks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rb.ks.builder.Builder;
import rb.ks.builder.config.Config;
import rb.ks.metrics.MetricsManager;

import static rb.ks.builder.config.Config.ConfigProperties.METRIC_ENABLE;

public class Normalizer {
    private static final Logger log = LoggerFactory.getLogger(Normalizer.class);

    public static void main(String[] args) throws Exception {
        if (args.length == 1) {
            Config config = new Config(args[0]);
            Builder builder = new Builder(config.clone());

            if (config.getOrDefault(METRIC_ENABLE, false)) {
                MetricsManager metricsManager = new MetricsManager(config.clone());
                metricsManager.start();

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    builder.close();
                    metricsManager.interrupt();
                    log.info("Stopped Normalizer process.");
                }));
            } else {
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    builder.close();
                    log.info("Stopped Normalizer process.");
                }));
            }

        } else {
            log.error("Execute: java -cp ${JAR_PATH} rb.ks.Normalizer <config_file>");
        }
    }
}
