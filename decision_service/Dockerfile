FROM python:3.11-slim

WORKDIR /app

# FIX: kafka-python-ng maintenu, flask version stable
RUN pip install --no-cache-dir \
    kafka-python-ng==2.2.3 \
    flask==3.0.3

COPY . .

EXPOSE 5000

CMD ["python", "decision_service.py"]
