package org.pingles.kafka;

import com.aphyr.riemann.Proto;
import com.aphyr.riemann.client.AbstractRiemannClient;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class RiemannReporter extends AbstractPollingReporter implements MetricProcessor<Metric> {
    private static final Logger LOGGER = Logger.getLogger(RiemannReporter.class);
    private final Clock clock;
    private final RiemannEventPublisher publisher;

    public RiemannReporter(Clock clock, RiemannEventPublisher publisher) {
        super(Metrics.defaultRegistry(), "riemann-reporter");
        this.clock = clock;
        this.publisher = publisher;
    }

    @Override
    public void run() {
        currentTime();
        final Set<Map.Entry<MetricName, Metric>> metrics = getMetricsRegistry().allMetrics().entrySet();
        for (Map.Entry<MetricName, Metric> metricEntry : metrics) {
            MetricName name = metricEntry.getKey();
            Metric metric = metricEntry.getValue();
            try {
                metric.processWith(this, name, metric);
            } catch (Exception e) {
                LOGGER.error("Couldn't process metric", e);
            }
        }
    }

    private Proto.Event.Builder buildEvent(String serviceLabel) {
        Proto.Event.Builder builder = Proto.Event.newBuilder();
        try {
            builder.setHost(InetAddress.getLocalHost().getCanonicalHostName());
            builder.setTime(currentTime());
            builder.setService(serviceLabel);
        } catch (UnknownHostException e) {
            LOGGER.error("Couldn't determine current host", e);
        }
        return builder;
    }

    @Override
    public void processMeter(MetricName name, Metered meter, Metric context) throws Exception {
        sendEvent(buildEvent(String.format("%s mean", name.getName())).setMetricD(meter.meanRate()).build());
    }

    @Override
    public void processCounter(MetricName name, Counter counter, Metric context) throws Exception {
        LOGGER.warn(String.format("Ignoring counter: %s", name.getName()));
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, Metric context) throws Exception {
        LOGGER.warn(String.format("Ignoring histogram: %s", name.getName()));
    }

    @Override
    public void processTimer(MetricName name, Timer timer, Metric context) throws Exception {
        LOGGER.warn(String.format("Ignoring timer: %s", name.getName()));
    }

    @Override
    public void processGauge(MetricName name, Gauge<?> gauge, Metric context) throws Exception {
        if (gauge.value() instanceof Double) {
            Proto.Event event = buildEvent(name.getName()).setMetricD((Double) gauge.value()).build();
            sendEvent(event);
        } else {
            Long longValue = Long.valueOf(gauge.value().toString());
            Proto.Event event = buildEvent(name.getName()).setMetricSint64(longValue).build();
            sendEvent(event);
        }
    }

    private void sendEvent(Proto.Event event) throws IOException {
        publisher.publish(event);
    }

    private long currentTime() {
        return TimeUnit.MILLISECONDS.toSeconds(clock.time());
    }
}
