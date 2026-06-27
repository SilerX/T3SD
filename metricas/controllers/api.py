from flask import request, jsonify


class APIController:
    def __init__(self, app, repository, calculator, backlog_monitor=None, publisher=None):
        self.app = app
        self.repo = repository
        self.calc = calculator
        self.backlog = backlog_monitor
        self.publisher = publisher
        self._setup()

    def _setup(self):
        @self.app.route('/health', methods=['GET'])
        def health():
            return jsonify({"status": "ok"}), 200

        @self.app.route('/record', methods=['POST'])
        def record():
            payload = request.json
            if payload:
                # Persistencia local (compatibilidad con Tarea 2)
                self.repo.save(payload)
                # T3: publicar el evento en metrics-topic para Spark
                if self.publisher is not None:
                    self.publisher.publish(payload)
            return jsonify({"status": "ok"}), 200

        @self.app.route('/stats', methods=['GET'])
        def stats():
            return jsonify(self.calc.calculate()), 200

        @self.app.route('/backlog', methods=['GET'])
        def backlog():
            if self.backlog is None:
                return jsonify({"error": "backlog monitor disabled"}), 503
            return jsonify(self.backlog.lag()), 200

        @self.app.route('/reset', methods=['POST'])
        def reset():
            self.repo.reset()
            return jsonify({"status": "reset"}), 200

        @self.app.route('/raw', methods=['GET'])
        def raw():
            limit = int(request.args.get('limit', '100'))
            events = self.repo.all()
            return jsonify(events[-limit:]), 200
