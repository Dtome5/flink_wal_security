FROM flink:latest

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    FLINK_CONF_DIR=${FLINK_HOME}/conf \
    PYFLINK_CLIENT_EXECUTABLE=python3 \
    XDG_DATA_HOME=/opt/uv/data

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libsnappy-dev \
        libsnappy1v5 \
        librocksdb-dev \
        libbz2-dev \
        liblz4-dev \
        libzstd-dev \
        libgflags-dev \
        ca-certificates \
        curl \
        jq \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY pyproject.toml ./
COPY uv.lock ./
RUN uv python pin 3.9.23
RUN uv sync --locked \
    && chmod -R a+rx /opt/uv /app/.venv \
    && chown -R flink:flink /opt/uv /app

RUN mkdir -p ${FLINK_CONF_DIR} \
    && mkdir -p /tmp/flink-savepoints \
    && mkdir -p /tmp/flink-checkpoints \
    && mkdir -p /tmp/rocksdb \
    && chown -R flink:flink /tmp/flink-savepoints /tmp/flink-checkpoints /tmp/rocksdb
COPY config.yaml ${FLINK_CONF_DIR}/config.yaml

COPY . .

# Compile the C++ swap tool without static linking
RUN g++ -std=c++17 swap_sst_last5.cpp \
    -o swap_sst_last5 \
    -lrocksdb -lz -lbz2 -lsnappy -llz4 -lzstd -pthread -ldl -lrt

RUN chmod +x run.sh swap_sst_last5 \
    && chown -R flink:flink /app

USER flink
CMD ["/app/run.sh", "/app/.venv/bin/python"]
