package rb.ks.mocks;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;

import java.io.File;

public class MockProcessContext implements ProcessorContext {
    @Override
    public String applicationId() {
        return null;
    }

    @Override
    public TaskId taskId() {
        return null;
    }

    @Override
    public Serde<?> keySerde() {
        return null;
    }

    @Override
    public Serde<?> valueSerde() {
        return null;
    }

    @Override
    public File stateDir() {
        return null;
    }

    @Override
    public StreamsMetrics metrics() {
        return null;
    }

    @Override
    public void register(StateStore store, boolean loggingEnabled, StateRestoreCallback stateRestoreCallback) {

    }

    @Override
    public StateStore getStateStore(String name) {
        return new MockKeyValueStore<>();
    }

    @Override
    public void schedule(long interval) {

    }

    @Override
    public <K, V> void forward(K key, V value) {

    }

    @Override
    public <K, V> void forward(K key, V value, int childIndex) {

    }

    @Override
    public <K, V> void forward(K key, V value, String childName) {

    }

    @Override
    public void commit() {

    }

    @Override
    public String topic() {
        return null;
    }

    @Override
    public int partition() {
        return 0;
    }

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public long timestamp() {
        return 0;
    }
}
