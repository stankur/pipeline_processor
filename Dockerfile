# Minimal container for the Flask API (invokes worker tasks on demand)
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (ca-certificates, curl for debugging if needed)
RUN apt-get update -y && apt-get install -y --no-install-recommends \
    ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

# Default port; overridden by env PORT
ENV PORT=8080
EXPOSE 8080

CMD ["python", "api.py"]

