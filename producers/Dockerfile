FROM python:3.11-slim

WORKDIR /app

# FIX: kafka-python-ng est maintenu et compatible Python 3.12+
# kafka-python 2.0.2 a des problèmes connus avec certaines versions Python
RUN pip install --no-cache-dir kafka-python-ng==2.2.3

COPY . .
