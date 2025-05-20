FROM bitnami/spark:latest

USER root
WORKDIR /app

# Copy only the requirements.txt from the root
COPY ../requirements.txt .

# Install Python dependencies, including Jupyter
RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    pip install notebook jupyterlab

# Copy the processing script
COPY ../processing/spark_job.py .

# Expose Jupyter port
EXPOSE 8888

# Default command: start Jupyter Lab
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--no-browser"]
