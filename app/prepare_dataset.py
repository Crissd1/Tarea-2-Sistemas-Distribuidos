import os
import pandas as pd


INPUT_FILE = "data/967_buildings.csv"
OUTPUT_FILE = "data/buildings_clean.csv"

REQUIRED_COLUMNS = [
    "latitude",
    "longitude",
    "area_in_meters",
    "confidence"
]

CHUNK_SIZE = 100_000


def clean_chunk(chunk):
    df_clean = chunk[REQUIRED_COLUMNS].copy()

    df_clean = df_clean.dropna()

    for col in REQUIRED_COLUMNS:
        df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce")

    df_clean = df_clean.dropna()

    df_clean = df_clean[
        (df_clean["confidence"] >= 0) &
        (df_clean["confidence"] <= 1)
    ]

    return df_clean


def main():
    print("Cargando dataset por partes...")

    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    total_original = 0
    total_clean = 0
    first_chunk = True

    reader = pd.read_csv(
        INPUT_FILE,
        usecols=REQUIRED_COLUMNS,
        chunksize=CHUNK_SIZE
    )

    for i, chunk in enumerate(reader, start=1):
        total_original += len(chunk)

        df_clean = clean_chunk(chunk)
        total_clean += len(df_clean)

        df_clean.to_csv(
            OUTPUT_FILE,
            mode="w" if first_chunk else "a",
            index=False,
            header=first_chunk
        )

        first_chunk = False

        print(f"Chunk {i} procesado | filas leídas: {total_original} | filas limpias: {total_clean}")

    print("Dataset limpio generado correctamente.")
    print(f"Archivo de salida: {OUTPUT_FILE}")
    print(f"Filas originales leídas: {total_original}")
    print(f"Filas limpias guardadas: {total_clean}")


if __name__ == "__main__":
    main()
