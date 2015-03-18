package dk.dbc.kafka.dispatch;


import com.beust.jcommander.JCommander;
import dk.dbc.kafka.dispatch.sources.StaticSource;
import dk.dbc.kafka.dispatch.sources.Source;
import dk.dbc.kafka.dispatch.sources.InputStreamSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Simple class that forwards every line of input to a Kafka server
 * @author Adam Tulinius
 */
public class KafkaDispatch {
    private final Properties properties;
    private final Source<String> source;

    public static void main(String[] args) throws IOException {
        CommandLineArgs commandLineArgs = new CommandLineArgs();
        JCommander jCommander = new JCommander(commandLineArgs, args);

        if (commandLineArgs.isHelp()) {
            jCommander.usage();
            return;
        }

        Properties properties = commandLineArgs.asProperties(System.getProperties());
        properties.putIfAbsent("linger.ms", 5);
        properties.putIfAbsent("acks", "all");

        Source<String> source;

        if (commandLineArgs.isBenchmark()) {
            int limit = commandLineArgs.getBenchmarkCount();
            source = new StaticSource(100, limit);
            System.out.println(String.format("Warning: Benchmark-mode enabled, limit=%s.", limit));
        } else {
            source = new InputStreamSource(System.in);
        }

        KafkaDispatch kafkaDispatch = new KafkaDispatch(properties, source);
        kafkaDispatch.run();
    }

    public KafkaDispatch(Properties properties, Source<String> source) {
        this.properties = properties;
        this.source = source;
    }

    public void run() throws IOException {
        StringSerializer stringSerializer = new StringSerializer();
        KafkaProducer<String, String> kafkaProducer
                = new KafkaProducer<>(properties, stringSerializer, stringSerializer);

        try {
            Optional<String> data;
            int count = 0;

            boolean chatty = Boolean.parseBoolean(properties.getProperty("kafkadispatch.verbose"));

            while (!(data = source.next()).equals(Optional.<String>empty())) {
                if (chatty) {
                    count++;
                    if (count % 100000 == 0) {
                        System.out.println(count);
                    }
                }

                kafkaProducer.send(new ProducerRecord<>(properties.getProperty("kafkadispatch.topic"), data.get().trim()));
            }

        } finally {
            kafkaProducer.close();

            if (Boolean.parseBoolean(properties.getProperty("kafkadispatch.showstats"))) {
                for (Map.Entry<MetricName, ? extends Metric> entry : kafkaProducer.metrics().entrySet()) {
                    System.out.println(String.format("%s: %s (%s)", entry.getKey().name(), entry.getValue().value(), entry.getKey().description()));
                }
            }
        }
    }
}
