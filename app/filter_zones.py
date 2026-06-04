import os
import pandas as pd

INPUT_FILE = "data/buildings_clean.csv"
OUTPUT_FILE = "data/buildings_zones.csv"

CHUNK_SIZE = 100_000

ZONES = {
    "Z1": {
        "name": "Providencia",
        "lat_min": -33.445,
        "lat_max": -33.420,
        "lon_min": -70.640,
        "lon_max": -70.600
    },
    "Z2": {
        "name": "Las Condes",
        "lat_min": -33.420,
        "lat_max": -33.390,
        "lon_min": -70.600,
        "lon_max": -70.550
    },
    "Z3": {
        "name": "Maipu",
        "lat_min": -33.530,
        "lat_max": -33.490,
        "lon_min": -70.790,
        "lon_max": -70.740
    },
    "Z4": {
        "name": "Santiago Centro",
        "lat_min": -33.460,
        "lat_max": -33.430,
        "lon_min": -70.670,
        "lon_max": -70.630
    },
    "Z5": {
        "name": "Pudahuel",
        "lat_min": -33.470,
        "lat_max": -33.430,
        "lon_min": -70.810,
        "lon_max": -70.760
    }
}


def assign_zone(df):
    result = []

    for zone_id, zone in ZONES.items():
        filtered = df[
            (df["latitude"] >= zone["lat_min"]) &
            (df["latitude"] <= zone["lat_max"]) &
            (df["longitude"] >= zone["lon_min"]) &
            (df["longitude"] <= zone["lon_max"])
        ].copy()

        if not filtered.empty:
            filtered["zone_id"] = zone_id
            filtered["zone_name"] = zone["name"]
            result.append(filtered)

    if result:
        return pd.concat(result, ignore_index=True)

    return pd.DataFrame()


def main():
    print("Filtrando registros por zonas de Santiago...")

    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    total_read = 0
    total_saved = 0
    first_chunk = True

    reader = pd.read_csv(INPUT_FILE, chunksize=CHUNK_SIZE)

    for i, chunk in enumerate(reader, start=1):
        total_read += len(chunk)

        filtered = assign_zone(chunk)
        total_saved += len(filtered)

        if not filtered.empty:
            filtered.to_csv(
                OUTPUT_FILE,
                mode="w" if first_chunk else "a",
                index=False,
                header=first_chunk
            )
            first_chunk = False

        print(
            f"Chunk {i} procesado | "
            f"filas leídas: {total_read} | "
            f"filas guardadas: {total_saved}"
        )

    print("Filtrado terminado.")
    print(f"Archivo generado: {OUTPUT_FILE}")
    print(f"Filas leídas: {total_read}")
    print(f"Filas guardadas en zonas: {total_saved}")


if __name__ == "__main__":
    main()
