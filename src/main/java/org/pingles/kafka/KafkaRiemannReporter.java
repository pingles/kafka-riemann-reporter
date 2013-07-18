package org.pingles.kafka;

import com.aphyr.riemann.client.AbstractRiemannClient;
import com.aphyr.riemann.client.RiemannClient;
import com.yammer.metrics.core.Clock;
import kafka.metrics.KafkaMetricsConfig;
import kafka.metrics.KafkaMetricsReporter;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class KafkaRiemannReporter implements KafkaRiemannReporterMBean, KafkaMetricsReporter {
    private boolean initialized = false;
    private final Object lock = new Object();
    private static final Logger LOGGER = Logger.getLogger(KafkaRiemannReporter.class);
    private RiemannReporter reporter;
    public KafkaRiemannReporter() {
    }

    @Override
    public void init(VerifiableProperties props) {
        synchronized (lock) {
            if (!initialized && isEnabled(props)) {
                initialize(props);
                initialized = true;
            }
        }
    }

    private void initialize(VerifiableProperties props) {
        KafkaMetricsConfig metricsConfig = new KafkaMetricsConfig(props);
        try {
            RiemannEventPublisher publisher = createPublisher(props);
            reporter = new RiemannReporter(Clock.defaultClock(), publisher);
            startReporter(metricsConfig.pollingIntervalSecs());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private RiemannEventPublisher createPublisher(VerifiableProperties props) throws IOException {
        String publisherType = props.getString("kafka.riemann.metrics.reporter.publisher");
        if (publisherType.equals("riemann")) {
            LOGGER.info("Connecting directly to Riemann");
            return RiemannTcpClientPublisher.buildFromProperties(props);
        }
        if (publisherType.equals("kafka")) {
            LOGGER.info("Sending Riemann events via. Kafka topic");
            return KafkaPublisher.buildFromProperties(props);
        }
        LOGGER.error(String.format("Invalid kafka.riemann.metrics.reporter.publisher setting: %s", publisherType));
        throw new RuntimeException("Didn't recognise kafka.riemann.metrics.reporter.publisher type.");
    }

    private boolean isEnabled(VerifiableProperties props) {
        return props.getBoolean("kafka.riemann.metrics.reporter.enabled", false);
    }

    @Override
    public void startReporter(long pollingPeriodInSeconds) {
        LOGGER.info(String.format("Starting Riemann metrics reporter, polling every %d seconds", pollingPeriodInSeconds));
        reporter.start(pollingPeriodInSeconds, TimeUnit.SECONDS);
    }

    @Override
    public void stopReporter() {
    }

    @Override
    public String getMBeanName() {
        return "kafka:type=org.pingles.kafka.KafkaRiemannReporter";
    }
}
