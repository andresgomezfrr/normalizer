package rb.ks.builder.bootstrap;

import rb.ks.builder.Builder;
import rb.ks.builder.config.Config;

public interface Bootstraper {
    void init(Builder builder, Config config) throws Exception;
    void close();
}
