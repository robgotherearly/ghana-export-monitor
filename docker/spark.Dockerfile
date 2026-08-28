# Spark image for both the streaming job and the nightly batch job.
#
# Built on pip-installed PySpark rather than a vendor image: it pins one exact
# Spark version, keeps the footprint small enough for an 8 GB laptop, and still
# ships the real spark-submit / start-master / start-worker scripts.
# bookworm on purpose: trixie dropped OpenJDK 17, and Spark 3.5 is only
# certified on Java 8/11/17 (Java 21 support landed in Spark 4).
FROM python:3.11-slim-bookworm

ARG SPARK_VERSION=3.5.3
ARG SCALA_VERSION=2.12
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL_VERSION=2.11.1
ARG POSTGRES_JDBC_VERSION=42.7.4
ARG DELTA_VERSION=3.2.1

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark \
    PYTHONPATH=/opt/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless curl procps tini \
    && rm -rf /var/lib/apt/lists/*

COPY docker/requirements-spark.txt /tmp/requirements.txt
RUN pip install --no-cache-dir pyspark==${SPARK_VERSION} -r /tmp/requirements.txt

ENV PATH=${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}

# Connector jars baked in at build time. Resolving them with --packages at
# startup would need Maven access on every container start, and a streaming job
# that cannot start without the internet is not much of a streaming job.
RUN set -eux; \
    cd "${SPARK_HOME}/jars"; \
    base=https://repo1.maven.org/maven2; \
    for url in \
      "${base}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
      "${base}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
      "${base}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "${base}/org/apache/commons/commons-pool2/${COMMONS_POOL_VERSION}/commons-pool2-${COMMONS_POOL_VERSION}.jar" \
      "${base}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      "${base}/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar" \
      "${base}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
    ; do curl -fsSL -O "$url"; done

WORKDIR /opt/app
COPY streaming ./streaming
COPY batch ./batch

RUN useradd --create-home --uid 10002 spark \
    && mkdir -p /data/lake /data/checkpoints \
    && chown -R spark:spark /data /opt/app
USER spark

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["spark-submit", "--driver-memory", "1500m", "/opt/app/streaming/stream_metrics.py"]
