# Airflow Docker Container
FROM apache/airflow
WORKDIR /
COPY /requirements.txt .
USER root

# Install Java

RUN sudo apt-get update && apt-get install -y curl && apt-get clean
RUN sudo apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Spark
RUN mkdir -p /opt/spark/jars/
RUN curl -L -o /opt/spark/jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
RUN curl -o /opt/spark/jars/gcs-connector-hadoop3-latest.jar \
    https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar


# Install Requirements.txt
RUN pip3 install -r requirements.txt
RUN pip3 install apache-airflow --upgrade
