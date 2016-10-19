package zz.ks.mocks;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockKeyValueStore<K, V> implements KeyValueStore<K, V> {
    private Map<K, V> cache = new HashMap<>();

    @Override
    public V get(K key) {
        return cache.get(key);
    }

    @Override
    public void put(K key, V value) {
        cache.put(key, value);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        V lastValue = cache.get(key);
        if(lastValue == null) cache.put(key, value);
        return lastValue;
    }

    @Override
    public void putAll(List<KeyValue<K, V>> entries) {
        for(KeyValue<K, V> entry : entries) cache.put(entry.key, entry.value);
    }

    @Override
    public V delete(K key) {
        return cache.remove(key);
    }

    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        return null;
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return null;
    }

    @Override
    public String name() {
        return "mock-key-value-store";
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {

    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {
        cache.clear();
    }

    @Override
    public boolean persistent() {
        return false;
    }
}
