FROM apache/airflow:2.6.1

# Become root to install requirements
USER root

RUN apt-get update && apt-get install -y libpq-dev python-dev
ADD --chown=airflow:airflow requirements.txt requirements.txt

USER airflow
RUN pip install -r requirements.txt --no-cache-dir