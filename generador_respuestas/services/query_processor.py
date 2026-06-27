import numpy as np
from models.zone import ZoneRepository


class QueryProcessor:
    def __init__(self, repository):
        self.repo = repository

    def process(self, query_type, zone_id, conf, zone_id_b=None, bins=5):
        zone = ZoneRepository.get_zone(zone_id)
        if not zone:
            raise ValueError(f"Zona invalida: {zone_id}")
        df = self.repo.get_data()
        filtro = (
            (df['latitude'].between(zone.lat_range[0], zone.lat_range[1])) &
            (df['longitude'].between(zone.lon_range[0], zone.lon_range[1])) &
            (df['confidence'] >= conf)
        )
        sub = df[filtro]

        if query_type == 'Q1':
            return int(len(sub))
        if query_type == 'Q2':
            return {
                "avg_area": float(sub['area_in_meters'].mean()) if not sub.empty else 0,
                "total_area": float(sub['area_in_meters'].sum()),
                "n": int(len(sub)),
            }
        if query_type == 'Q3':
            return float(len(sub) / zone.area_km2)
        if query_type == 'Q4':
            zone_b = ZoneRepository.get_zone(zone_id_b)
            if zone_b is None:
                raise ValueError(f"Zona B invalida: {zone_id_b}")
            filtro_b = (
                (df['latitude'].between(zone_b.lat_range[0], zone_b.lat_range[1])) &
                (df['longitude'].between(zone_b.lon_range[0], zone_b.lon_range[1])) &
                (df['confidence'] >= conf)
            )
            dA = len(sub) / zone.area_km2
            dB = len(df[filtro_b]) / zone_b.area_km2
            return {
                "zone_a": zone_id, "zone_b": zone_id_b,
                "dens_a": dA, "dens_b": dB,
                "winner": zone_id if dA > dB else zone_id_b,
            }
        if query_type == 'Q5':
            freqs, edges = np.histogram(sub['confidence'], bins=bins, range=(0, 1))
            return [
                {"bucket": i, "min": float(edges[i]), "max": float(edges[i+1]), "count": int(freqs[i])}
                for i in range(len(freqs))
            ]
        raise ValueError(f"Tipo de consulta no soportado: {query_type}")
