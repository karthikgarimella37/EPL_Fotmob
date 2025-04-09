FROM apache/airflow
WORKDIR /

COPY /requirements.txt .
USER root
RUN apt-get update && apt-get install -y curl && apt-get clean
RUN mkdir -p /opt/spark/jars/

RUN curl -L -o /opt/spark/jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar


RUN pip3 install -r requirements.txt
RUN pip3 install apache-airflow --upgrade
