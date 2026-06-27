import os


class Config:
    DATASET_PATH = os.getenv('DATASET_PATH', '/app/data/967_buildings.csv.gz')
    FAILURE_RATE = float(os.getenv('FAILURE_RATE', '0.0'))
    LATENCY_MS = float(os.getenv('LATENCY_MS', '0'))
    DOWN_AT = float(os.getenv('DOWN_AT', '-1'))
    DOWN_DURATION = float(os.getenv('DOWN_DURATION', '0'))
    PORT = int(os.getenv('PORT', '5000'))
