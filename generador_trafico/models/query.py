import time
import uuid


class Query:
    def __init__(self, tipo, zone_id, confidence_min, zone_id_b=None, bins=None):
        self.id = str(uuid.uuid4())
        self.tipo = tipo
        self.zone_id = zone_id
        self.confidence_min = confidence_min
        self.zone_id_b = zone_id_b
        self.bins = bins
        self.retry_count = 0
        self.created_at = time.time()

    def to_dict(self):
        d = {
            "id": self.id,
            "type": self.tipo,
            "zone_id": self.zone_id,
            "confidence_min": self.confidence_min,
            "retry_count": self.retry_count,
            "created_at": self.created_at,
        }
        if self.zone_id_b is not None:
            d["zone_id_b"] = self.zone_id_b
        if self.bins is not None:
            d["bins"] = self.bins
        return d
