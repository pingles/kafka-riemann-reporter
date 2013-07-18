package org.pingles.kafka;

import com.aphyr.riemann.Proto;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

/**
 * Publishes Riemann Proto.Event events to a Kafka topic.
 */
public class KafkaPublisher implements RiemannEventPublisher {
    private static final Logger LOGGER = Logger.getLogger(KafkaPublisher.class);
    private final Producer producer;
    private final String topic;

    public KafkaPublisher(Producer producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void publish(Proto.Event event) throws IOException {
        producer.send(new KeyedMessage(topic, event.toByteArray()));
    }

    public static RiemannEventPublisher buildFromProperties(VerifiableProperties props) throws UnknownHostException {
        String topic = props.getString("kafka.riemann.metrics.reporter.publisher.topic", "riemann_event");
        String connectionString = brokerConnectionString(props);

        LOGGER.info(String.format("Broker connection: %s. Topic: %s", connectionString, topic));

        Producer producer = crateProducer(connectionString);

        return new KafkaPublisher(producer, topic);
    }

    private static String brokerConnectionString(VerifiableProperties props) throws UnknownHostException {
        Integer port = props.getInt("port", 9092);
        String host = InetAddress.getLocalHost().getCanonicalHostName();
        if (props.containsKey("host.name")) {
            host = props.getString("host.name");
        }
        return String.format("%s:%d", host, port);
    }

    private static Producer crateProducer(String connectionString) {
        Properties producerProps = new Properties();
        producerProps.put("metadata.broker.list", connectionString);
        producerProps.put("serializer.class", "kafka.serializer.DefaultEncoder");
        ProducerConfig config = new ProducerConfig(producerProps);
        return new Producer(config);
    }
}
