import os


class Config:
    KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092')
    TOPIC_MAIN = os.getenv('TOPIC_MAIN', 'queries.main')
    TOPIC_RETRY = os.getenv('TOPIC_RETRY', 'queries.retry')
    TOPIC_DLQ = os.getenv('TOPIC_DLQ', 'queries.dlq')
    CONSUMER_GROUP = os.getenv('CONSUMER_GROUP', 'consumidor-grupo')

    REDIS_HOST = os.getenv('REDIS_HOST', 'redis-cache')
    REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
    CACHE_TTL = int(os.getenv('CACHE_TTL', '3600'))

    URL_RESPUESTA = os.getenv('URL_RESPUESTA', 'http://generador-respuestas:5000')
    URL_METRICAS = os.getenv('URL_METRICAS', 'http://metricas:5001')

    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
    RETRY_BACKOFF_MS = int(os.getenv('RETRY_BACKOFF_MS', '500'))

    CONSUMER_ID = os.getenv('CONSUMER_ID', 'c1')
    MODE = os.getenv('MODE', 'main')
