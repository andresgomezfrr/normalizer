package io.wizzie.ks.normalizer;

import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.ks.normalizer.builder.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Normalizer {
    private static final Logger log = LoggerFactory.getLogger(Normalizer.class);

    public static void main(String[] args) throws Exception {
        if (args.length == 1) {
            Config config = new Config(args[0]);
            Builder builder = new Builder(config.clone());

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    builder.close();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
                log.info("Stopped Normalizer process.");
            }));


        } else {
            log.error("Execute: java -cp ${JAR_PATH} io.wizzie.ks.normalizer.Normalizer <config_file>");
        }
    }
}
