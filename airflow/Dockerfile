FROM python:3.12.3-slim

# ============================================================
# 基本環境變數設定
# ============================================================
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    AIRFLOW_HOME=/opt/airflow

# ============================================================
# 安裝系統依賴 (Airflow + Pandas + PyArrow 需要)
# ============================================================
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    default-libmysqlclient-dev \
    libsasl2-dev \
    libssl-dev \
    libffi-dev \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ============================================================
# 建立 airflow 使用者與目錄
# ============================================================
RUN useradd -ms /bin/bash airflow && \
    mkdir -p ${AIRFLOW_HOME}/dags && \
    mkdir -p ${AIRFLOW_HOME}/logs && \
    mkdir -p ${AIRFLOW_HOME}/plugins && \
    mkdir -p ${AIRFLOW_HOME}/app && \
    chown -R airflow:airflow ${AIRFLOW_HOME}

WORKDIR ${AIRFLOW_HOME}

# ============================================================
# 安裝 Airflow + 專案 requirements（含 constraints）
# ============================================================
ARG AIRFLOW_VERSION=2.10.2
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# 專案根目錄的 requirements.txt
COPY requirements.txt /tmp/requirements.txt

RUN pip install --upgrade pip && \
    pip install "apache-airflow[postgres]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}" && \
    # pip install "apache-airflow[postgres]==${AIRFLOW_VERSION}" && \
    pip install -r /tmp/requirements.txt

# ============================================================
# 切換為 airflow user
# ============================================================
USER airflow

# ============================================================
# 設定 Python import path（src/ 會掛載到 /opt/airflow/app/src）
# ============================================================
ENV PYTHONPATH="${PYTHONPATH}:${AIRFLOW_HOME}/app"

# ============================================================
# Airflow 基礎設定
# ============================================================
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__EXECUTOR=LocalExecutor

# ============================================================
# 預設執行方式（可被 docker-compose 覆寫）
# ============================================================
CMD ["airflow", "version"]
