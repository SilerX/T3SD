FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python numpy requests
COPY . .
CMD ["python", "-u", "main.py"]
