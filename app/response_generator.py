import math
import pandas as pd
import numpy as np
from zones import ZONES


class ResponseGenerator:
    def __init__(self, dataset_path="data/buildings_zones.csv"):
        print("Cargando dataset de zonas en memoria...")

        self.df = pd.read_csv(dataset_path)

        self.data_by_zone = {
            zone_id: self.df[self.df["zone_id"] == zone_id]
            for zone_id in ZONES.keys()
        }

        self.zone_area_km2 = {
            zone_id: self.calculate_bbox_area_km2(zone)
            for zone_id, zone in ZONES.items()
        }

        print("Dataset cargado correctamente.")
        print(f"Total de registros cargados: {len(self.df)}")

        for zone_id, data in self.data_by_zone.items():
            zone_name = ZONES[zone_id]["name"]
            print(f"{zone_id} - {zone_name}: {len(data)} registros")

    def calculate_bbox_area_km2(self, zone):
        lat_min = zone["lat_min"]
        lat_max = zone["lat_max"]
        lon_min = zone["lon_min"]
        lon_max = zone["lon_max"]

        lat_distance = (lat_max - lat_min) * 111.0

        avg_lat = (lat_min + lat_max) / 2
        lon_distance = (lon_max - lon_min) * 111.0 * math.cos(math.radians(avg_lat))

        return abs(lat_distance * lon_distance)

    def q1_count(self, zone_id, confidence_min=0.0):
        data = self.data_by_zone[zone_id]

        filtered = data[data["confidence"] >= confidence_min]

        return {
            "query": "Q1",
            "zone_id": zone_id,
            "zone_name": ZONES[zone_id]["name"],
            "confidence_min": confidence_min,
            "count": int(len(filtered))
        }

    def q2_area(self, zone_id, confidence_min=0.0):
        data = self.data_by_zone[zone_id]

        filtered = data[data["confidence"] >= confidence_min]

        if len(filtered) == 0:
            avg_area = 0.0
            total_area = 0.0
        else:
            avg_area = float(filtered["area_in_meters"].mean())
            total_area = float(filtered["area_in_meters"].sum())

        return {
            "query": "Q2",
            "zone_id": zone_id,
            "zone_name": ZONES[zone_id]["name"],
            "confidence_min": confidence_min,
            "avg_area": avg_area,
            "total_area": total_area,
            "n": int(len(filtered))
        }

    def q3_density(self, zone_id, confidence_min=0.0):
        count_result = self.q1_count(zone_id, confidence_min)

        area_km2 = self.zone_area_km2[zone_id]

        if area_km2 == 0:
            density = 0.0
        else:
            density = count_result["count"] / area_km2

        return {
            "query": "Q3",
            "zone_id": zone_id,
            "zone_name": ZONES[zone_id]["name"],
            "confidence_min": confidence_min,
            "area_km2": area_km2,
            "count": count_result["count"],
            "density_buildings_km2": density
        }

    def q4_compare_density(self, zone_a, zone_b, confidence_min=0.0):
        density_a = self.q3_density(zone_a, confidence_min)
        density_b = self.q3_density(zone_b, confidence_min)

        if density_a["density_buildings_km2"] > density_b["density_buildings_km2"]:
            winner = zone_a
        elif density_b["density_buildings_km2"] > density_a["density_buildings_km2"]:
            winner = zone_b
        else:
            winner = "tie"

        return {
            "query": "Q4",
            "zone_a": zone_a,
            "zone_a_name": ZONES[zone_a]["name"],
            "zone_b": zone_b,
            "zone_b_name": ZONES[zone_b]["name"],
            "confidence_min": confidence_min,
            "density_a": density_a["density_buildings_km2"],
            "density_b": density_b["density_buildings_km2"],
            "winner": winner
        }

    def q5_confidence_dist(self, zone_id, bins=5):
        data = self.data_by_zone[zone_id]

        scores = data["confidence"].dropna().to_numpy()

        counts, edges = np.histogram(scores, bins=bins, range=(0, 1))

        distribution = []

        for i in range(bins):
            distribution.append({
                "bucket": int(i),
                "min": float(edges[i]),
                "max": float(edges[i + 1]),
                "count": int(counts[i])
            })

        return {
            "query": "Q5",
            "zone_id": zone_id,
            "zone_name": ZONES[zone_id]["name"],
            "bins": bins,
            "distribution": distribution
        }

    def process_query(self, query):
        query_type = query["query_type"]

        if query_type == "Q1":
            return self.q1_count(
                query["zone_id"],
                query.get("confidence_min", 0.0)
            )

        if query_type == "Q2":
            return self.q2_area(
                query["zone_id"],
                query.get("confidence_min", 0.0)
            )

        if query_type == "Q3":
            return self.q3_density(
                query["zone_id"],
                query.get("confidence_min", 0.0)
            )

        if query_type == "Q4":
            return self.q4_compare_density(
                query["zone_id"],
                query["zone_b"],
                query.get("confidence_min", 0.0)
            )

        if query_type == "Q5":
            return self.q5_confidence_dist(
                query["zone_id"],
                query.get("bins", 5)
            )

        raise ValueError(f"Tipo de consulta no reconocido: {query_type}")
