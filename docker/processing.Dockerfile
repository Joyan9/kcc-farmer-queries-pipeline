FROM bitnami/spark:latest

USER root
WORKDIR /app

# Find and use the correct Python executable used by Spark
ENV SPARK_HOME=/opt/bitnami/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*.zip:/app

# Copy requirements first
COPY ../requirements.txt .

# Use the Python executable that comes with Spark
RUN /opt/bitnami/python/bin/pip install --upgrade pip && \
    /opt/bitnami/python/bin/pip install -r requirements.txt && \
    /opt/bitnami/python/bin/pip install notebook jupyterlab

# Verify pyspark is available (it should be pre-installed in Bitnami Spark)
RUN /opt/bitnami/python/bin/python -c "import pyspark; print('PySpark version:', pyspark.__version__)"

COPY ../processing/*.py .
COPY ../processing/helpers ./helpers

EXPOSE 8888
EXPOSE 4040

# Use the correct Python executable
CMD ["/opt/bitnami/python/bin/jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--no-browser"]