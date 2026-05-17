FROM apache/airflow:2.8.4-python3.11

# Switch to root to install system packages
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/java-17-openjdk 2>/dev/null || true

# JAVA_HOME required by PySpark — arch-agnostic symlink works on amd64 and arm64
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Switch back to the airflow user for pip installs
USER airflow

WORKDIR /opt/airflow

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source packages so DAGs can import from src.*
COPY src/ /opt/airflow/src/
COPY pyproject.toml /opt/airflow/pyproject.toml

# Install the local package in editable mode so `src.*` imports resolve
RUN pip install --no-cache-dir -e /opt/airflow

# DAGs are mounted at runtime via docker-compose volume (see docker-compose.yaml)
