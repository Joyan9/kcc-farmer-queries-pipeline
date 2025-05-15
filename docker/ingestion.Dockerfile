FROM python:3.10-slim

WORKDIR /app

COPY ../requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY ../ingestion/ .

CMD ["python", "main.py"]
