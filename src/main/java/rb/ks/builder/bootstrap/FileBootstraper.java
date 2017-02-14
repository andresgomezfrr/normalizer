package rb.ks.builder.bootstrap;

import rb.ks.builder.Builder;
import rb.ks.builder.config.Config;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Files;

public class FileBootstraper extends ThreadBootstraper {
    public static final String FILE_PATH = "file.bootstraper.path";
    @Override
    public void run() {
        //Nothing to do
    }

    @Override
    public void init(Builder builder, Config config) throws Exception {
        String filePath = config.get(FILE_PATH);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));

        StringBuilder stringBuffer = new StringBuilder();
        String line;

        while ((line = bufferedReader.readLine()) != null) {

            stringBuffer.append(line).append("\n");
        }

        builder.updateStreamConfig(stringBuffer.toString());
    }

    @Override
    public void close() {
        //Nothing to do
    }
}
