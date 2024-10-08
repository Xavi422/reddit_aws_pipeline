FROM apache/airflow:2.10.0-python3.11

COPY requirements.txt /opt/airflow/

USER root
# necessary for installing and compiling python packages
RUN apt-get update && apt-get install -y gcc python3-dev

USER airflow

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt