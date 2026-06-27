import time
from flask import request, jsonify


class APIController:
    def __init__(self, app, processor, failure):
        self.app = app
        self.processor = processor
        self.failure = failure
        self._setup()

    def _setup(self):
        @self.app.route('/health', methods=['GET'])
        def health():
            return jsonify({"status": "ok"}), 200

        @self.app.route('/query', methods=['POST', 'GET'])
        def handle_query():
            if request.method == 'GET':
                return jsonify({"status": "ready"}), 200

            t0 = time.time()
            self.failure.inject_latency()

            if self.failure.should_fail():
                return jsonify({"error": "simulated_failure"}), 503

            data = request.json or {}
            tipo = data.get('type')
            zona = data.get('zone_id')
            conf = float(data.get('confidence_min', 0.0))
            zona_b = data.get('zone_id_b')
            bins = int(data.get('bins', 5)) if data.get('bins') else 5

            try:
                result = self.processor.process(tipo, zona, conf, zona_b, bins)
                return jsonify({
                    "result": result,
                    "latency_ms": (time.time() - t0) * 1000.0,
                }), 200
            except ValueError as e:
                return jsonify({"error": str(e)}), 400
            except Exception as e:
                return jsonify({"error": f"internal: {type(e).__name__}: {e}"}), 500
