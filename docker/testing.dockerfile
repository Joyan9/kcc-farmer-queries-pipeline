FROM bitnami/spark:latest

USER root
WORKDIR /app

ENV SPARK_HOME=/opt/bitnami/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*.zip:/app

# Install Python dependencies for tests
COPY ../requirements.txt .
RUN /opt/bitnami/python/bin/pip install --upgrade pip && \
    /opt/bitnami/python/bin/pip install -r requirements.txt && \
    /opt/bitnami/python/bin/pip install pytest

# Copy app source and tests
COPY ../processing/*.py ./processing/
COPY ../processing/helpers ./processing/helpers
COPY ../tests ./tests
# in docker/testing.dockerfile
COPY ../ingestion ./ingestion
ENV PYTHONPATH="/app:$PYTHONPATH"


# Run tests using Spark's Python
CMD ["/opt/bitnami/python/bin/pytest", "tests"]
