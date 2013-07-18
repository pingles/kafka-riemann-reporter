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
    private AbstractRiemannClient riemannClient;

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
            String host = props.getString("kafka.riemann.metrics.reporter.host", "127.0.0.1");
            Integer port = props.getInt("kafka.riemann.metrics.reporter.port", 5555);
            riemannClient = RiemannClient.tcp(host, port);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        reporter = new RiemannReporter(Clock.defaultClock(), riemannClient);
        startReporter(metricsConfig.pollingIntervalSecs());
    }

    private boolean isEnabled(VerifiableProperties props) {
        return props.getBoolean("kafka.riemann.metrics.reporter.enabled", false);
    }

    @Override
    public void startReporter(long pollingPeriodInSeconds) {
        LOGGER.info(String.format("Starting Riemann metrics reporter, polling every %d seconds", pollingPeriodInSeconds));
        try {
            riemannClient.connect();

            reporter.start(pollingPeriodInSeconds, TimeUnit.SECONDS);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stopReporter() {
        if (riemannClient != null && riemannClient.isConnected()) {
            try {
                riemannClient.disconnect();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public String getMBeanName() {
        return "kafka:type=org.pingles.kafka.KafkaRiemannReporter";
    }
}
