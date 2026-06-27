FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir flask pandas numpy gunicorn
COPY . .
CMD ["gunicorn", "-b", "0.0.0.0:5000", "-w", "2", "--threads", "4", "--timeout", "60", "main:app"]
