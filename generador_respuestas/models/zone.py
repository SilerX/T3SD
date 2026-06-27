class Zone:
    def __init__(self, id_zone, name, lat_range, lon_range, area_km2):
        self.id_zone = id_zone
        self.name = name
        self.lat_range = lat_range
        self.lon_range = lon_range
        self.area_km2 = area_km2


class ZoneRepository:
    ZONES = {
        "Z1": Zone("Z1", "Providencia", (-33.445, -33.420), (-70.640, -70.600), 5.2),
        "Z2": Zone("Z2", "Las Condes", (-33.420, -33.390), (-70.600, -70.550), 7.8),
        "Z3": Zone("Z3", "Maipu", (-33.530, -33.490), (-70.790, -70.740), 12.4),
        "Z4": Zone("Z4", "Santiago Centro", (-33.460, -33.430), (-70.670, -70.630), 4.5),
        "Z5": Zone("Z5", "Pudahuel", (-33.470, -33.430), (-70.810, -70.760), 10.1),
    }

    @classmethod
    def get_zone(cls, zone_id):
        return cls.ZONES.get(zone_id)

    @classmethod
    def get_all_zones(cls):
        return cls.ZONES
