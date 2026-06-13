# IoT data generator — local stand-in for an AWS ECS producer task
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Run as non-root, like a sensible production task definition
RUN useradd --create-home generator
USER generator

CMD ["python", "main.py"]
