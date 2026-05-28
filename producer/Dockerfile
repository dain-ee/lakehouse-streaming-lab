FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY config /app/config
COPY src /app/src

CMD ["python", "src/order_event_producer.py"]
