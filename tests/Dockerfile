FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/tests/requirements.txt
RUN pip install --no-cache-dir -r /app/tests/requirements.txt

COPY . /app/tests
