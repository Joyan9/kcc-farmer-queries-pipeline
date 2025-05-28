FROM python:3.10-slim

USER root
WORKDIR /app

# Copy requirements.txt from the root
COPY requirements.txt .

# Install Python dependencies (including Jupyter and viz libs)
RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    pip install notebook jupyterlab

# Copy notebooks from the visualization directory
COPY visualization/notebooks/ ./notebooks/

EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--no-browser", "--notebook-dir=/app/notebooks", "--NotebookApp.default_url=/lab/tree/dashboard.ipynb"]
