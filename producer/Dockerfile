FROM python:3.9-slim

WORKDIR /app
COPY producer.py /app/

RUN pip install --no-cache-dir kafka-python

CMD ["python", "-u", "/app/producer.py"]
