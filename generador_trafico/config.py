import os


class Config:
    KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092')
    TOPIC_MAIN = os.getenv('TOPIC_MAIN', 'queries.main')
    DISTRIBUCION = os.getenv('DIST', 'zipf')
    N_CONSULTAS = int(os.getenv('N_CONSULTAS', '1000'))
    RATE = float(os.getenv('RATE', '50'))
    SPIKE = int(os.getenv('SPIKE', '0'))
    SPIKE_FACTOR = int(os.getenv('SPIKE_FACTOR', '10'))
    SPIKE_AT = float(os.getenv('SPIKE_AT', '0.5'))
    SPIKE_DURATION = float(os.getenv('SPIKE_DURATION', '5'))
    URL_METRICAS = os.getenv('URL_METRICAS', 'http://metricas:5001')
