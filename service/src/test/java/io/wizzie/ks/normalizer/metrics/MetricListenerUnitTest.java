package io.wizzie.ks.normalizer.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.ks.normalizer.builder.config.ConfigProperties;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class MetricListenerUnitTest {

    private final Config config = new Config();
    private final ObjectMapper mapper = new ObjectMapper();

    @Before
    public void initTest() {
        config
                .put(ConfigProperties.METRIC_ENABLE, true)
                .put(ConfigProperties.METRIC_INTERVAL, 2000)
                .put(StreamsConfig.APPLICATION_ID_CONFIG, "testing-metric-manager")
                .put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
                .put(ConfigProperties.METRIC_LISTENERS, Collections.singletonList("io.wizzie.ks.normalizer.metrics.MockMetricListener"));
    }

    @Test
    public void metricManagerShouldHaveAMetricListener() {
        MetricsManager metricsManager = new MetricsManager(config);

        assertFalse(metricsManager.listeners.isEmpty());

        assertTrue(metricsManager.listeners.get(0) instanceof MockMetricListener);
    }

    @Test
    public void metricManagerShouldSendMetricsToMetricListener() throws InterruptedException, JsonProcessingException {
        MetricsManager metricsManager = new MetricsManager(config);

        assertEquals(1, metricsManager.listeners.size());

        MetricListener metricListener = metricsManager.listeners.get(0);

        assertTrue(metricListener instanceof MockMetricListener);

        MockMetricListener mockMetricListener = (MockMetricListener) metricListener;

        List<Map<String, Object>> expectedList = new ArrayList<>();

        Counter counter = new Counter();
        counter.inc(5);
        mockMetricListener.updateMetric("counter-test", counter.getCount());

        Map<String, Object> expectedResponse = new HashMap<>();
        expectedResponse.put("monitor", "counter-test");
        expectedResponse.put("value", counter.getCount());
        expectedResponse.put("timestamp",  System.currentTimeMillis() / 1000L);

        expectedList.add(expectedResponse);

        Meter meter = new Meter();
        meter.mark();
        meter.mark();
        meter.mark();

        mockMetricListener.updateMetric("meter-test", meter.getCount());

        expectedResponse = new HashMap<>();
        expectedResponse.put("monitor", "meter-test");
        expectedResponse.put("value", meter.getCount());
        expectedResponse.put("timestamp",  System.currentTimeMillis() / 1000L);

        expectedList.add(expectedResponse);

        assertEquals(expectedList, mockMetricListener.content);
    }
}
