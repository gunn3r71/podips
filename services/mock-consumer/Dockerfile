FROM python:3.9.0-alpine

WORKDIR /app 

COPY requirements.txt /app/
COPY mock-consumer.py /app/

RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "mock-consumer.py"]