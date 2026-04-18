# Sử dụng Python 3.10 để tương thích tốt nhất
FROM apache/airflow:2.8.1-python3.10

USER root

# Cài đặt git và các thư viện hệ thống cần thiết cho một số python packages (nếu có)
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         git \
         curl \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# TẠO THƯ MỤC TEMP_DATA VÀ CẤP QUYỀN
# Điều này cực kỳ quan trọng để logic "Extract to Disk" trong cukcuk_pipeline.py không bị lỗi Permission Denied
RUN mkdir -p /opt/airflow/temp_data && chown -R airflow:root /opt/airflow/temp_data

USER airflow

# Nâng cấp pip để tránh lỗi khi cài đặt các thư viện mới
RUN pip install --upgrade pip

# Cài đặt các thư viện từ requirements.txt
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt