import os
import pandas as pd
from models.zone import ZoneRepository


class DatasetRepository:
    def __init__(self, file_path):
        self.file_path = file_path
        self.df = None

    def load_data(self):
        if not os.path.exists(self.file_path):
            print(f"[WARN] Dataset no encontrado en {self.file_path}, generando dataset sintetico")
            self.df = self._synthetic()
            return
        print("[INFO] Cargando dataset a RAM...")
        cols = ['latitude', 'longitude', 'area_in_meters', 'confidence']
        df = pd.read_csv(self.file_path, compression='gzip', usecols=cols)
        mask = pd.Series(False, index=df.index)
        for _, zone in ZoneRepository.get_all_zones().items():
            m = (df['latitude'].between(zone.lat_range[0], zone.lat_range[1])) & \
                (df['longitude'].between(zone.lon_range[0], zone.lon_range[1]))
            mask |= m
        self.df = df[mask].copy()
        print(f"[OK] Dataset listo: {len(self.df)} registros")

    def _synthetic(self):
        import numpy as np
        rows = []
        for _, z in ZoneRepository.get_all_zones().items():
            n = 1000
            lat = np.random.uniform(z.lat_range[0], z.lat_range[1], n)
            lon = np.random.uniform(z.lon_range[0], z.lon_range[1], n)
            area = np.random.uniform(20, 400, n)
            conf = np.random.uniform(0.0, 1.0, n)
            for i in range(n):
                rows.append({
                    'latitude': lat[i],
                    'longitude': lon[i],
                    'area_in_meters': area[i],
                    'confidence': conf[i],
                })
        return pd.DataFrame(rows)

    def get_data(self):
        return self.df
