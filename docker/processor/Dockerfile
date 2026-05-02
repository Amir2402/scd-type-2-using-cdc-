FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/load_changes_to_staging/load_events.py .

CMD ["python", "src/load_changes_to_staging/load_events.py"]
