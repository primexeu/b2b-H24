# syntax=docker/dockerfile:1

FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["sh", "-c", "gunicorn -w ${GUNICORN_WORKERS:-2} -b 0.0.0.0:${PORT:-5000} app:app"]
