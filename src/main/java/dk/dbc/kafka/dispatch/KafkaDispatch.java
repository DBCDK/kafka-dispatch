package dk.dbc.kafka.dispatch;


import com.beust.jcommander.JCommander;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

/**
 * Simple class that forwards every line of input to a Kafka server
 * @author Adam Tulinius
 */
public class KafkaDispatch {
    private final Properties properties;
    private InputStream inputStream;

    public static void main(String[] args) throws IOException {
        CommandLineArgs commandLineArgs = new CommandLineArgs();
        JCommander jCommander = new JCommander(commandLineArgs, args);

        if (commandLineArgs.isHelp()) {
            jCommander.usage();
        } else {

            Properties properties = commandLineArgs.asProperties(System.getProperties());
            properties.putIfAbsent("linger.ms", 5);
            properties.putIfAbsent("acks", "all");

            KafkaDispatch kafkaDispatch = new KafkaDispatch(properties, System.in);
            kafkaDispatch.run();
        }
    }

    public KafkaDispatch(Properties properties, InputStream inputStream) {
        this.properties = properties;
        this.inputStream = inputStream;
    }

    public void run() throws IOException {
        StringSerializer stringSerializer = new StringSerializer();
        KafkaProducer<String, String> kafkaProducer
                = new KafkaProducer<>(properties, stringSerializer, stringSerializer);

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            String line;
            int count = 0;

            boolean chatty = Boolean.parseBoolean(properties.getProperty("kafkadispatch.verbose"));

            while ((line = reader.readLine()) != null) {
                if (chatty) {
                    count++;
                    if (count % 100000 == 0) {
                        System.out.println(count);
                    }
                }

                kafkaProducer.send(new ProducerRecord<>(properties.getProperty("kafkadispatch.topic"), line.trim()));
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
