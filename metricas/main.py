from flask import Flask
from config import Config
from repositories.metrics_repository import MetricsRepository
from services.stats_calculator import StatsCalculator
from services.backlog_monitor import BacklogMonitor
from producers.metrics_publisher import MetricsPublisher
from controllers.api import APIController


class MetricsApp:
    def __init__(self, config):
        self.config = config

    def build(self):
        app = Flask(__name__)
        repo = MetricsRepository(self.config.REDIS_HOST, self.config.REDIS_PORT, self.config.METRICS_KEY)
        calc = StatsCalculator(repo)
        try:
            backlog = BacklogMonitor(
                self.config.KAFKA_BOOTSTRAP,
                [self.config.TOPIC_MAIN, self.config.TOPIC_RETRY, self.config.TOPIC_DLQ],
                self.config.CONSUMER_GROUP,
            )
        except Exception as e:
            print(f"[WARN] Backlog monitor no inicializado: {e}")
            backlog = None

        publisher = None
        if self.config.PUBLISH_METRICS:
            try:
                publisher = MetricsPublisher(self.config.KAFKA_BOOTSTRAP, self.config.TOPIC_METRICS)
            except Exception as e:
                print(f"[WARN] MetricsPublisher no inicializado: {e}")
                publisher = None

        APIController(app, repo, calc, backlog, publisher)
        return app


app = MetricsApp(Config).build()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=Config.PORT)
