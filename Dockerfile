FROM apache/airflow:2.5.1-python3.9

USER root

RUN apt-get update && apt-get install -y \
    python3-dev \
    libpq-dev \
    openjdk-11-jdk \
    wget \
    && apt-get clean

RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.24.jar -P /opt/spark/jars/
RUN ls -l /opt/spark/jars/

USER airflow

COPY scripts/airflow-entrypoint.sh /scripts/airflow-entrypoint.sh
COPY airflow/requirements.txt /requirements.txt

RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt