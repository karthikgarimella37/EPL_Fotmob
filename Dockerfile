FROM python3

WORKDIR /

COPY /requirements.txt .
RUN pip3 install -r requirements.txt

RUN mkdir -p /opt/spark/jars/

RUN curl -L -o /opt/spark/jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

