package rb.ks.builder.bootstrap;

import rb.ks.builder.Builder;
import rb.ks.builder.config.Config;

import java.nio.file.Files;

public class FileBootstraper extends ThreadBootstraper {
    public static final String FILE_PATH = "file.bootstraper.path";
    @Override
    public void run() {
        //Nothing to do
    }

    @Override
    public void init(Builder builder, Config config) throws Exception {
        String streamConfig = new String(Files.readAllBytes(config.get(FILE_PATH)));
        builder.updateStreamConfig(streamConfig);
    }

    @Override
    public void close() {
        //Nothing to do
    }
}
