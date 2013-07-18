package org.pingles.kafka;

import com.aphyr.riemann.Proto;
import com.aphyr.riemann.client.RiemannClient;
import kafka.utils.VerifiableProperties;

import java.io.IOException;

public class RiemannTcpClientPublisher implements RiemannEventPublisher {
    private final RiemannClient riemann;

    public RiemannTcpClientPublisher(RiemannClient riemann) {
        this.riemann = riemann;
    }

    @Override
    public void publish(Proto.Event event) throws IOException {
        riemann.sendEventsWithAck(event);
    }

    private void connect() throws IOException {
        this.riemann.connect();
    }

    public static RiemannTcpClientPublisher buildFromProperties(VerifiableProperties props) throws IOException {
        String host = props.getString("kafka.riemann.metrics.reporter.host", "127.0.0.1");
        Integer port = props.getInt("kafka.riemann.metrics.reporter.port", 5555);

        RiemannTcpClientPublisher publisher = new RiemannTcpClientPublisher(RiemannClient.tcp(host, port));
        publisher.connect();

        return publisher;
    }
}
