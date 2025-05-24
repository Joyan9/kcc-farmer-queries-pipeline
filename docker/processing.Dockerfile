FROM bitnami/spark:latest

USER root
WORKDIR /app

COPY ../requirements.txt .

RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    pip install notebook jupyterlab

# Copy all scripts in processing/
COPY ../processing/*.py .

EXPOSE 8888
EXPOSE 4040

# Default command: start Jupyter Lab
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--no-browser"]
