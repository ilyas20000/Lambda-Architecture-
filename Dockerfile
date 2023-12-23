FROM apache/airflow:latest
USER root
RUN apt-get update && \
    apt-get -y install git && \
    apt-get -y install openjdk-17-jdk && \
    apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

USER airflow
COPY requirement_airflow.txt /requirement_airflow.txt

RUN pip install --upgrade pip \
    && pip install --no-cache-dir  -r /requirement_airflow.txt



