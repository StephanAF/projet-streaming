FROM python:3.9-slim-bullseye

# Installer Java 11 (stable et compatible)
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless wget && \
    rm -rf /var/lib/apt/lists/*

# Télécharger et installer Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Configurer les variables d'environnement Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Installer les dépendances Python
RUN pip install --no-cache-dir \
    kafka-python \
    pyspark==${SPARK_VERSION} \
    pandas \
    numpy

WORKDIR /app
COPY streaming_app.py /app/

CMD ["python3", "/app/streaming_app.py"]
