package dk.dbc.kafka.dispatch.sources;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Source for reading InputStreams line-by-line
 * @author Adam Tulinius
 */
public class InputStreamSource extends Source<String> {
    private BufferedReader reader;

    public InputStreamSource(InputStream inputStream) {
        this.reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
    }

    @Override
    public Optional<String> next() throws IOException {
        String line = reader.readLine();
        if (line != null) {
            return Optional.of(line);
        } else {
            return Optional.empty();
        }
    }
}
