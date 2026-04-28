# Airflow 3.1.6 with Python 3.12
FROM apache/airflow:3.1.6-python3.12

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    openjdk-17-jre-headless \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Spark (Java 17 works with Spark 4.0 and 4.1)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Download and install Spark (for spark-submit)
ENV SPARK_VERSION=4.1.0
ENV SPARK_HOME=/opt/spark
RUN curl -fsSL --retry 3 --retry-delay 15 --connect-timeout 30 --max-time 1500 \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    | tar -xz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} \
    && chown -R airflow:root ${SPARK_HOME}

ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER airflow

# Install Airflow providers (latest versions compatible with Airflow 3.x)
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==5.5.0 \
    apache-airflow-providers-postgres==5.14.0 \
    apache-airflow-providers-apache-kafka==1.8.0 \
    apache-airflow-providers-docker==3.14.0 \
    apache-airflow-providers-http==5.6.3 \
    pyspark==4.1.0

# Create directories
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/config

WORKDIR /opt/airflow
