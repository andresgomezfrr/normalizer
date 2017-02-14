package rb.ks.builder.bootstrap;

import rb.ks.builder.Builder;

import java.util.Map;

public interface Bootstraper {
    void init(Builder builder, Map<String, String> properties) throws Exception;
    void close();
}
