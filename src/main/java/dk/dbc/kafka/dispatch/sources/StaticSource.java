package dk.dbc.kafka.dispatch.sources;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/**
 * Source for benchmarking with static data
 * @author Adam Tulinius
 */
public class StaticSource extends Source<String> {
    private int limit;
    private int sent = 0;
    private String data;

    public StaticSource(int dataSize, int limit) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("benchmark:");

        while (stringBuilder.length() < dataSize) {
            stringBuilder.append(UUID.randomUUID());
        }

        data = stringBuilder.substring(0, dataSize);
        this.limit = limit;
    }

    @Override
    public Optional<String> next() throws IOException {
        if (limit == 0 || sent < this.limit) {
            sent++;
            return Optional.of(data);
        } else {
            return Optional.empty();
        }
    }
}
