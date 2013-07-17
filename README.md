# Kafka Riemann Reporter
Kafka includes support for automatically reporting various metrics (messages, bytes per topic etc.) in it's [`kafka.metrics.KafkaCSVMetricsReporter`](https://svn.apache.org/repos/asf/kafka/trunk/core/src/main/scala/kafka/metrics/KafkaCSVMetricsReporter.scala). This library provides an alternative which creates [Riemann](http://riemann.io) events.

Events are created and published back to Kafka using a configured topic (`riemann_event` by default).
