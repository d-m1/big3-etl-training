FROM apache/airflow:2.6.3

USER root
RUN apt-get update && apt-get install -y python3-pip openjdk-11-jdk
RUN pip install pyspark psycopg2-binary

USER airflow