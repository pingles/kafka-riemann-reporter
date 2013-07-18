package org.pingles.kafka;

import com.aphyr.riemann.Proto;

import java.io.IOException;

public interface RiemannEventPublisher {
    public void publish(Proto.Event event) throws IOException;
}
