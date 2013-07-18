# Kafka Riemann Reporter
Kafka includes support for automatically reporting various metrics (messages, bytes per topic etc.) in it's [`kafka.metrics.KafkaCSVMetricsReporter`](https://svn.apache.org/repos/asf/kafka/trunk/core/src/main/scala/kafka/metrics/KafkaCSVMetricsReporter.scala). This library provides an alternative which creates [Riemann](http://riemann.io) events.

Events are created and published back to Kafka using a configured topic (`riemann_event` by default).

## Using

Build the JAR and copy it to your Kafka install's `./libs` directory.

$ mvn package
$ cp target/kafka-riemann-reporter-0.1-SNAPSHOT-jar-with-dependencies.jar $KAFKA_HOME/libs

Then edit the `server.properties` and set the following:

    kafka.metrics.polling.interval.secs=5
    kafka.metrics.reporters=org.pingles.kafka.KafkaRiemannReporter
    kafka.riemann.metrics.reporter.enabled=true
    kafka.riemann.metrics.reporter.host=127.0.0.1
    kafka.riemann.metrics.reporter.port=5555
