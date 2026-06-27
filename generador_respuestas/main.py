from flask import Flask
from config import Config
from repositories.dataset_repository import DatasetRepository
from services.query_processor import QueryProcessor
from services.failure_simulator import FailureSimulator
from controllers.api import APIController


class ResponseGeneratorApp:
    def __init__(self, config):
        self.config = config

    def build(self):
        app = Flask(__name__)
        repo = DatasetRepository(self.config.DATASET_PATH)
        repo.load_data()
        processor = QueryProcessor(repo)
        failure = FailureSimulator(
            failure_rate=self.config.FAILURE_RATE,
            latency_ms=self.config.LATENCY_MS,
            down_at=self.config.DOWN_AT,
            down_duration=self.config.DOWN_DURATION,
        )
        APIController(app, processor, failure)
        return app


app = ResponseGeneratorApp(Config).build()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=Config.PORT)
