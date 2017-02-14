package rb.ks.metrics;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rb.ks.builder.config.Config;
import rb.ks.utils.ConversionUtils;

import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static rb.ks.builder.config.Config.ConfigProperties.*;

public class MetricsManager extends Thread {
    private static final Logger log = LoggerFactory.getLogger(MetricsManager.class);
    MetricRegistry registry = new MetricRegistry();
    AtomicBoolean running = new AtomicBoolean(true);
    List<MetricListener> listeners = new ArrayList<>();
    Set<String> registredMetrics = new HashSet<>();
    Config config;
    Long interval;

    public MetricsManager(Config config) {
        this.config = config;
        this.interval = ConversionUtils.toLong(config.getOrDefault(METRIC_INTERVAL, 60000L));
        List<String> listenersClass = config.get(METRIC_LISTENERS);
        if (listenersClass != null) {
            for (String listenerClassName : listenersClass) {
                try {
                    Class listenerClass = Class.forName(listenerClassName);
                    MetricListener metricListener = (MetricListener) listenerClass.newInstance();
                    metricListener.init(config.clone());
                    listeners.add(metricListener);
                } catch (ClassNotFoundException e) {
                    log.error("Couldn't find the class associated with the metric listener {}", listenerClassName);
                } catch (InstantiationException | IllegalAccessException e) {
                    log.error("Couldn't create the instance associated with the metric listener " + listenerClassName, e);
                }
            }
        }

        registerMetrics();
        log.info("Start MetricsManager with listeners {}",
                listeners.stream()
                        .map(MetricListener::name).collect(Collectors.toList()));

        if (listeners.isEmpty()) {
            log.warn("Stop MetricsManager because doesn't have listeners!!");
            running.set(false);
        }
    }

    @Override
    public void run() {
        while (running.get()) {
            sendAllMetrics();
            try {
                Thread.sleep(interval);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private void registerMetrics() {
        registerAll("gc", new GarbageCollectorMetricSet());
        registerAll("buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
        registerAll("memory", new MemoryUsageGaugeSet());
        registerAll("threads", new ThreadStatesGaugeSet());
    }

    private void registerAll(String prefix, MetricSet metricSet) {
        for (Map.Entry<String, Metric> entry : metricSet.getMetrics().entrySet()) {
            if (entry.getValue() instanceof MetricSet) {
                registerAll(prefix + "." + entry.getKey(), (MetricSet) entry.getValue());
            } else {
                registry.register(prefix + "." + entry.getKey(), entry.getValue());
            }
        }
    }

    public void registerMetric(String metricName, Metric metric) {
        if (!registredMetrics.contains(metricName)) {
            registry.register(metricName, metric);
            registredMetrics.add(metricName);
        } else {
            log.warn("The metric with name [{}] is duplicated!", metric);
        }
    }

    public void removeMetric(String metricName) {
        if (registredMetrics.contains(metricName)) {
            registry.remove(metricName);
            registredMetrics.remove(metricName);
        } else {
            log.warn("Try to delete unregister metric [{}]", metricName);
        }
    }

    public void clean() {
        registredMetrics.forEach(metric -> registry.remove(metric));
        registredMetrics.clear();
    }

    private void sendMetric(String metricName) {
        Object value = registry.getGauges().get(metricName).getValue();
        listeners.forEach(listener -> listener.updateMetric(metricName, value));
    }

    private void sendAllMetrics() {
        registry.getGauges().keySet().forEach(this::sendMetric);
    }

    @Override
    public void interrupt() {
        running.set(false);
        listeners.forEach(MetricListener::close);
        log.info("Stop MetricsManager");
    }
}
