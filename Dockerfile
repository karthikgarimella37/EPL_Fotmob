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

RUN curl -sSL https://sdk.cloud.google.com | bash
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && apt-get update -y && apt-get install google-cloud-cli -y
    
RUN gsutil --version
# Install Requirements.txt
RUN pip3 install -r requirements.txt
RUN pip3 install apache-airflow --upgrade

COPY /airflow/dags/terraform_keys.json /opt/airflow/terraform_keys.json

RUN pip3 uninstall -y python-dotenv && \
    pip3 install -U python-dotenv

RUN mkdir opt/deps && \
pip install -t opt/deps -r /requirements.txt && \
cd deps && zip -r ../deps.zip .