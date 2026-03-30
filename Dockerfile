# Sử dụng Python 3.10 để tương thích tốt nhất
FROM apache/airflow:2.8.1-python3.10

USER root
# Cài đặt git và các tools cơ bản
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         git \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Cài đặt các thư viện từ requirements.txt
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt