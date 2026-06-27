FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir flask redis numpy kafka-python gunicorn
COPY . .
CMD ["gunicorn", "-b", "0.0.0.0:5001", "-w", "2", "--threads", "4", "--timeout", "60", "main:app"]
