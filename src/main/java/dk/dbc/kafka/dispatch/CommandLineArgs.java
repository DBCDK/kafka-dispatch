package dk.dbc.kafka.dispatch;

import com.beust.jcommander.Parameter;

import java.util.Properties;

/**
 * Configuration option parsing for KafkaDispatch
 * @author Adam Tulinius
 */
public class CommandLineArgs {
    @Parameter(names = {"-h", "--help"}, description = "display this help message", help = true)
    private boolean help;

    @Parameter(names = {"-s", "--servers"}, description = "servers to connect to, delimited by comma", required = true)
    private String servers;

    @Parameter(names = {"-t", "--topic"}, description = "topic to write to", required = true)
    private String topic;

    @Parameter(names = {"-v"}, description = "turn on output", required = false)
    private boolean verbose;

    @Parameter(names = {"-d", "--dump-stats"}, description = "print stats when done", required = false)
    private boolean stats;

    public String toString() {
        return String.format("servers: %s, topic: %s", servers, topic);
    }

    public boolean isHelp() {
        return help;
    }

    public String getServers() {
        return servers;
    }

    public String getTopic() {
        return topic;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public boolean printStats() {
        return stats;
    }

    public Properties asProperties() {
        return asProperties(new Properties());
    }

    public Properties asProperties(Properties properties) {
        properties.put("bootstrap.servers", servers);
        properties.put("kafkadispatch.topic", topic);
        properties.put("kafkadispatch.verbose", "" + verbose);
        properties.put("kafkadispatch.showstats", "" + stats);
        return properties;
    }
}
