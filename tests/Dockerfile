FROM python:3.11-slim

WORKDIR /tests

COPY requirements.txt /tests/requirements.txt
RUN pip install --no-cache-dir -r /tests/requirements.txt

COPY . /tests
