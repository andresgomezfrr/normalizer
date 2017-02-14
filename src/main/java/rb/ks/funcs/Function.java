package rb.ks.funcs;

import java.util.Map;

public interface Function<R> {
    void init(Map<String, Object> properties);
    void prepare(Map<String, Object> properties);
    R process(String key, Map<String, Object> value);
    void stop();
}
