from fastapi import FastAPI, HTTPException
import numpy as np
import pandas as pd
import os
import random
import time

app = FastAPI()

# ── Configuración ─────────────────────────────────────────────────────────────

FAILURE_RATE  = float(os.getenv("FAILURE_RATE", "0.0"))  # 0.0 a 1.0
LATENCIA_MS   = float(os.getenv("LATENCIA_MS",  "0.0"))  # latencia artificial
CSV_PATH      = os.getenv("CSV_PATH", "/data/open_buildings_rm.csv")

# ── Zonas predefinidas ────────────────────────────────────────────────────────

ZONAS = {
    "Z1": {"lat_min": -33.445, "lat_max": -33.420,
           "lon_min": -70.640, "lon_max": -70.600, "area_km2": 14.4},
    "Z2": {"lat_min": -33.420, "lat_max": -33.390,
           "lon_min": -70.600, "lon_max": -70.550, "area_km2": 99.4},
    "Z3": {"lat_min": -33.530, "lat_max": -33.490,
           "lon_min": -70.790, "lon_max": -70.740, "area_km2": 133.0},
    "Z4": {"lat_min": -33.460, "lat_max": -33.430,
           "lon_min": -70.670, "lon_max": -70.630, "area_km2": 22.4},
    "Z5": {"lat_min": -33.470, "lat_max": -33.430,
           "lon_min": -70.810, "lon_max": -70.760, "area_km2": 197.0},
}

# ── Carga del dataset en memoria ──────────────────────────────────────────────

def cargar_datos():
    """
    Carga el CSV real y filtra por zona usando bounding boxes.
    Si el CSV no existe, usa datos simulados para desarrollo.
    """
    if not os.path.exists(CSV_PATH):
        print(f"⚠️  CSV no encontrado en {CSV_PATH}, usando datos simulados")
        return generar_datos_simulados()

    print(f"Cargando dataset desde {CSV_PATH}...")
    df = pd.read_csv(CSV_PATH, usecols=["latitude", "longitude",
                                         "area_in_meters", "confidence"])
    df = df.dropna()

    datos = {}
    for zona_id, bounds in ZONAS.items():
        filtro = (
            (df["latitude"]  >= bounds["lat_min"]) &
            (df["latitude"]  <= bounds["lat_max"]) &
            (df["longitude"] >= bounds["lon_min"]) &
            (df["longitude"] <= bounds["lon_max"])
        )
        subset = df[filtro][["confidence", "area_in_meters"]].values.tolist()
        datos[zona_id] = subset
        print(f"  {zona_id}: {len(subset)} edificios cargados")

    return datos

def generar_datos_simulados():
    """Datos de respaldo para desarrollo sin el CSV."""
    rng = np.random.default_rng(42)
    return {
        zona_id: list(zip(
            rng.uniform(0.3, 1.0, 500).tolist(),   # confidence
            rng.uniform(50, 400, 500).tolist()       # area_in_meters
        ))
        for zona_id in ZONAS
    }

# Carga al iniciar — queda en memoria durante toda la ejecución
data = cargar_datos()

# ── Helper: simular fallo y latencia ─────────────────────────────────────────

def verificar_fallo():
    """Lanza 503 según FAILURE_RATE. Usado en todos los endpoints."""
    if LATENCIA_MS > 0:
        time.sleep(LATENCIA_MS / 1000)
    if FAILURE_RATE > 0 and random.random() < FAILURE_RATE:
        raise HTTPException(status_code=503, detail="Fallo simulado")

# ── Endpoints Q1–Q5 ───────────────────────────────────────────────────────────

@app.get("/q1/{zone_id}")
def q1_count(zone_id: str, confidence_min: float = 0.0):
    """Conteo de edificios en una zona."""
    verificar_fallo()
    if zone_id not in data:
        raise HTTPException(status_code=404, detail="Zona no encontrada")

    count = sum(1 for conf, _ in data[zone_id] if conf >= confidence_min)
    return {"zone": zone_id, "count": count}


@app.get("/q2/{zone_id}")
def q2_area(zone_id: str, confidence_min: float = 0.0):
    """Área promedio y área total de edificaciones."""
    verificar_fallo()
    if zone_id not in data:
        raise HTTPException(status_code=404, detail="Zona no encontrada")

    areas = [area for conf, area in data[zone_id] if conf >= confidence_min]
    if not areas:
        return {"avg_area": 0, "total_area": 0, "n": 0}

    return {
        "avg_area":   sum(areas) / len(areas),
        "total_area": sum(areas),
        "n":          len(areas)
    }


@app.get("/q3/{zone_id}")
def q3_density(zone_id: str, confidence_min: float = 0.0):
    """Densidad de edificaciones por km²."""
    verificar_fallo()
    if zone_id not in data:
        raise HTTPException(status_code=404, detail="Zona no encontrada")

    count    = sum(1 for conf, _ in data[zone_id] if conf >= confidence_min)
    area_km2 = ZONAS[zone_id]["area_km2"]
    return {"zone": zone_id, "density": count / area_km2}


@app.get("/q4/{zone_a}/{zone_b}")
def q4_compare(zone_a: str, zone_b: str, confidence_min: float = 0.0):
    """Comparación de densidad entre dos zonas."""
    verificar_fallo()
    for z in [zone_a, zone_b]:
        if z not in data:
            raise HTTPException(status_code=404, detail=f"Zona {z} no encontrada")

    da = sum(1 for c, _ in data[zone_a] if c >= confidence_min) / ZONAS[zone_a]["area_km2"]
    db = sum(1 for c, _ in data[zone_b] if c >= confidence_min) / ZONAS[zone_b]["area_km2"]
    return {"zone_a": da, "zone_b": db, "winner": zone_a if da > db else zone_b}


@app.get("/q5/{zone_id}")
def q5_confidence_dist(zone_id: str, bins: int = 5):
    """Distribución de confianza en una zona."""
    verificar_fallo()
    if zone_id not in data:
        raise HTTPException(status_code=404, detail="Zona no encontrada")

    scores         = [conf for conf, _ in data[zone_id]]
    counts, edges  = np.histogram(scores, bins=bins, range=(0.0, 1.0))

    return {
        "zone": zone_id,
        "distribution": [
            {
                "bucket": i,
                "min":    round(float(edges[i]),   3),
                "max":    round(float(edges[i+1]), 3),
                "count":  int(counts[i])
            }
            for i in range(bins)
        ]
    }


@app.get("/health")
def health():
    return {"status": "ok", "zonas_cargadas": list(data.keys())}