package io.wizzie.ks.normalizer.funcs.impl.debug;

import io.wizzie.ks.normalizer.funcs.MapperFunction;
import io.wizzie.ks.normalizer.metrics.MetricsManager;
import org.apache.kafka.streams.KeyValue;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import java.util.Map;

public class LogMapper extends MapperFunction {

    Logger log;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        String logName = (String) properties.getOrDefault("logname", "log");

        log = Logger.getLogger(logName);

        RollingFileAppender fileAppender = new RollingFileAppender();
        fileAppender.setMaxFileSize((String) properties.getOrDefault("maxfilesize", "5MB"));
        fileAppender.setMaxBackupIndex((Integer) properties.getOrDefault("maxbackupindex", 10));
        fileAppender.setFile((String) properties.getOrDefault("filepath", String.format("/var/log/ks-normalizer/debug/%s.log", logName)));

        PatternLayout patternLayout = new PatternLayout();
        patternLayout.setConversionPattern((String) properties.getOrDefault("conversionpattern", "%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1} - %m%n"));

        fileAppender.setLayout(patternLayout);

        log.addAppender(fileAppender);
        log.setLevel(Level.INFO);
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {
        log.info(String.format("KEY: %s - VALUE: %s", key, value));
        return new KeyValue<>(key, value);
    }

    @Override
    public void stop() {

    }
}
