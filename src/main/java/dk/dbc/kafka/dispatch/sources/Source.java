package dk.dbc.kafka.dispatch.sources;

import java.io.IOException;
import java.util.Optional;

/**
 * Base class for data inputs
 * @author Adam Tulinius
 */
public abstract class Source<T> {
    public abstract Optional<T> next() throws IOException;
}
