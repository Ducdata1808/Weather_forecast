# Sử dụng base image là Airflow bản mới nhất
FROM apache/airflow:2.8.2

# 1. Cài đặt các thư viện lõi của hệ điều hành (Đặc biệt là Java 17)
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Thiết lập biến môi trường trỏ tới Java 17 vừa cài
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 2. Cài đặt các thư viện Python của dự án bạn
USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
