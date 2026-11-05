# One image for every BikeFlow Python component: the ingestion services and
# the teammates' Spark jobs / dashboard. Same Python, Java, Spark and Delta
# everywhere.
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    JAVA_HOME=/opt/java \
    HOME=/tmp \
    BIKEFLOW_DATA=/app/data

# Java 17 for Spark; procps because Spark's launch scripts call `ps`.
RUN mkdir -p /usr/share/man/man1 \
 && apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java \
 && java -version

# Default user matches most laptops (UID 1000).
RUN useradd --uid 1000 --create-home --shell /bin/bash app

WORKDIR /app

COPY requirements.txt requirements-dev.txt ./
RUN pip install -r requirements-dev.txt

# Download the Delta Lake + Spark-Kafka jars once, at build time.
COPY scripts/bake_spark_jars.py scripts/
RUN python scripts/bake_spark_jars.py

COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-deps -e . && bikeflow-ingest --help > /dev/null

COPY scripts ./scripts
COPY tests ./tests

CMD ["bikeflow-ingest", "--help"]
