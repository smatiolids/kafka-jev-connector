FROM eclipse-temurin:17-jdk AS plugin
COPY target/kafka-jev-connector-0.1-plugin.zip /tmp/plugin.zip
RUN mkdir /plugin && cd /plugin && jar xf /tmp/plugin.zip

FROM apache/kafka:4.2.0
USER root
COPY --from=plugin /plugin/kafka-jev-connector-0.1 /opt/kafka/plugins/kafka-jev-connector
COPY local-kafka/connect-distributed.properties /opt/kafka/config/connect-distributed.properties
