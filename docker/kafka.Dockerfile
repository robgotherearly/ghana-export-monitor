# Stock Apache Kafka, with the KRaft log directory pre-created and owned by the
# image's unprivileged user - otherwise Docker creates the named-volume mount
# point as root and the broker cannot write its own metadata.
FROM apache/kafka:3.8.1

USER root
RUN mkdir -p /var/lib/kafka/data && chown -R 1000:1000 /var/lib/kafka
USER 1000
