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
            builder.addTags("kafkabroker");
        } catch (UnknownHostException e) {
            LOGGER.error("Couldn't determine current host", e);
        }
        return builder;
    }

    @Override
    public void processMeter(MetricName name, Metered meter, Metric context) throws Exception {
        Proto.Event.Builder builder = buildEvent(String.format("%s mean", name.getName()));
        Proto.Event event = builder.setMetricD(meter.meanRate())
                .addAttributes(buildMetricTypeAttribute("meter"))
                .build();
        sendEvent(event);
    }

    @Override
    public void processCounter(MetricName name, Counter counter, Metric context) throws Exception {
        Proto.Event.Builder builder = buildEvent(name.getName());
        Proto.Event event = builder.setMetricSint64(counter.count())
                .addAttributes(buildMetricTypeAttribute("counter"))
                .build();
        sendEvent(event);
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, Metric context) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("Ignoring histogram: %s", name.getName()));
        }
    }

    @Override
    public void processTimer(MetricName name, Timer timer, Metric context) throws Exception {
        Proto.Event.Builder builder = buildEvent(String.format("%s mean", name.getName()));
        Proto.Event event = builder.setMetricD(timer.mean())
                .addAttributes(buildMetricTypeAttribute("timer"))
                .build();
        sendEvent(event);

        builder = buildEvent(String.format("%s one minute", name.getName()));
        event = builder.setMetricD(timer.oneMinuteRate())
                .addAttributes(buildMetricTypeAttribute("timer"))
                .build();
        sendEvent(event);
    }

    @Override
    public void processGauge(MetricName name, Gauge<?> gauge, Metric context) throws Exception {
        Proto.Event.Builder builder = buildEvent(name.getName());
        builder.addAttributes(buildMetricTypeAttribute("gauge"));

        if (gauge.value() instanceof Double) {
            Proto.Event event = builder.setMetricD((Double) gauge.value()).build();
            sendEvent(event);
        } else {
            Long longValue = Long.valueOf(gauge.value().toString());
            Proto.Event event = builder.setMetricSint64(longValue).build();
            sendEvent(event);
        }
    }

    private Proto.Attribute buildMetricTypeAttribute(String type) {
        return Proto.Attribute.newBuilder().setKey("metricType").setValue(type).build();
    }

    private void sendEvent(Proto.Event event) throws IOException {
        publisher.publish(event);
    }

    private long currentTime() {
        return TimeUnit.MILLISECONDS.toSeconds(clock.time());
    }
}
