from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional, List
import time
import json
import os
import numpy as np
from collections import deque

app = FastAPI()

# ── Modelo de evento ──────────────────────────────────────────────────────────

class MetricEvent(BaseModel):
    # Campos base (T1)
    tipo:        str    # hit | miss | error | retry | dlq | recovery
    consulta:    str    # Q1..Q5
    latencia_ms: float

    # Campos nuevos (T2)
    zona:        Optional[str]   = None
    msg_id:      Optional[str]   = None
    retry_count: Optional[int]   = 0
    backoff_seg: Optional[float] = None
    detalle:     Optional[str]   = None

# ── Almacenamiento en memoria + archivo ──────────────────────────────────────

LOGS_PATH    = os.getenv("LOGS_PATH", "/data/metricas.jsonl")
logs_eventos: List[dict] = []   # lista principal
inicio_sistema = time.time()    # para calcular throughput global

def guardar_en_disco(evento: dict):
    """Persiste cada evento en un archivo JSONL (una línea por evento)."""
    try:
        os.makedirs(os.path.dirname(LOGS_PATH), exist_ok=True)
        with open(LOGS_PATH, "a") as f:
            f.write(json.dumps(evento) + "\n")
    except Exception as e:
        print(f"Error guardando en disco: {e}")

# ── Endpoints ────────────────────────────────────────────────────────────────

@app.post("/log")
def registrar_evento(evento: MetricEvent):
    """Recibe un evento desde cualquier servicio y lo guarda."""
    registro = evento.model_dump()
    registro["timestamp"] = time.time()   # agregamos timestamp al guardar
    logs_eventos.append(registro)
    guardar_en_disco(registro)
    return {"status": "ok"}


@app.get("/stats")
def obtener_estadisticas():
    """Calcula todas las métricas requeridas por T1 y T2."""
    if not logs_eventos:
        return {"mensaje": "sin datos aún"}

    total      = len(logs_eventos)
    hits       = [e for e in logs_eventos if e["tipo"] == "hit"]
    misses     = [e for e in logs_eventos if e["tipo"] == "miss"]
    retries    = [e for e in logs_eventos if e["tipo"] == "retry"]
    dlqs       = [e for e in logs_eventos if e["tipo"] == "dlq"]
    errors     = [e for e in logs_eventos if e["tipo"] == "error"]
    recoveries = [e for e in logs_eventos if e["tipo"] == "recovery"]

    # ── Métricas T1 ──
    total_consultas = len(hits) + len(misses)
    hit_rate  = len(hits) / total_consultas if total_consultas > 0 else 0.0
    miss_rate = 1 - hit_rate

    # Latencia p50 / p95
    latencias = [e["latencia_ms"] for e in logs_eventos
                 if e["tipo"] in ("hit", "miss") and e["latencia_ms"] > 0]
    p50 = float(np.percentile(latencias, 50)) if latencias else 0.0
    p95 = float(np.percentile(latencias, 95)) if latencias else 0.0

    # Throughput: consultas procesadas exitosamente por segundo
    tiempo_total = time.time() - inicio_sistema
    throughput   = total_consultas / tiempo_total if tiempo_total > 0 else 0.0

    # ── Métricas T2 ──
    retry_rate    = len(retries)    / total if total > 0 else 0.0
    dlq_rate      = len(dlqs)       / total if total > 0 else 0.0
    recovery_rate = len(recoveries) / total if total > 0 else 0.0

    # Recovery time: tiempo entre primer error y última recovery
    recovery_time_seg = None
    if errors and recoveries:
        primer_error    = min(e["timestamp"] for e in errors)
        ultima_recovery = max(e["timestamp"] for e in recoveries)
        if ultima_recovery > primer_error:
            recovery_time_seg = round(ultima_recovery - primer_error, 2)

    return {
        # T1
        "total_eventos":       total,
        "hits":                len(hits),
        "misses":              len(misses),
        "hit_rate":            round(hit_rate,  4),
        "miss_rate":           round(miss_rate, 4),
        "latencia_p50_ms":     round(p50, 2),
        "latencia_p95_ms":     round(p95, 2),
        "throughput_qps":      round(throughput, 2),

        # T2
        "retries":             len(retries),
        "dlq_total":           len(dlqs),
        "errores":             len(errors),
        "recoveries":          len(recoveries),
        "retry_rate":          round(retry_rate,    4),
        "dlq_rate":            round(dlq_rate,      4),
        "recovery_rate":       round(recovery_rate, 4),
        "recovery_time_seg":   recovery_time_seg,

        # General
        "uptime_seg":          round(tiempo_total, 1),
    }


@app.get("/stats/por-consulta")
def stats_por_consulta():
    """Desglosa hit rate y latencia p95 por tipo de consulta (Q1–Q5)."""
    resultado = {}
    for qtype in ["Q1", "Q2", "Q3", "Q4", "Q5"]:
        eventos_q = [e for e in logs_eventos if e["consulta"] == qtype]
        hits_q    = [e for e in eventos_q if e["tipo"] == "hit"]
        misses_q  = [e for e in eventos_q if e["tipo"] == "miss"]
        total_q   = len(hits_q) + len(misses_q)
        lats_q    = [e["latencia_ms"] for e in eventos_q if e["latencia_ms"] > 0]

        resultado[qtype] = {
            "total":       total_q,
            "hit_rate":    round(len(hits_q) / total_q, 4) if total_q > 0 else 0.0,
            "latencia_p95_ms": round(float(np.percentile(lats_q, 95)), 2)
                               if lats_q else 0.0,
        }
    return resultado


@app.get("/stats/por-zona")
def stats_por_zona():
    """Desglosa hit rate por zona geográfica (Z1–Z5)."""
    resultado = {}
    for zona in ["Z1", "Z2", "Z3", "Z4", "Z5"]:
        eventos_z = [e for e in logs_eventos if e.get("zona") == zona]
        hits_z    = [e for e in eventos_z if e["tipo"] == "hit"]
        total_z   = len(eventos_z)
        resultado[zona] = {
            "total":    total_z,
            "hit_rate": round(len(hits_z) / total_z, 4) if total_z > 0 else 0.0,
        }
    return resultado


@app.get("/stats/timeline")
def stats_timeline(ventana_seg: int = 10):
    """
    Throughput en ventanas de tiempo. Útil para graficar
    cómo evoluciona el sistema durante spikes o fallos.
    """
    if not logs_eventos:
        return []

    ahora      = time.time()
    inicio     = min(e["timestamp"] for e in logs_eventos)
    duracion   = ahora - inicio
    n_ventanas = max(1, int(duracion / ventana_seg))

    resultado = []
    for i in range(n_ventanas):
        t_inicio = inicio + i * ventana_seg
        t_fin    = t_inicio + ventana_seg
        ventana  = [e for e in logs_eventos
                    if t_inicio <= e["timestamp"] < t_fin]
        exitos   = [e for e in ventana if e["tipo"] in ("hit", "miss")]
        retries_v = [e for e in ventana if e["tipo"] == "retry"]

        resultado.append({
            "ventana":        i + 1,
            "t_inicio":       round(t_inicio - inicio, 1),
            "t_fin":          round(t_fin    - inicio, 1),
            "consultas":      len(exitos),
            "throughput_qps": round(len(exitos) / ventana_seg, 2),
            "retries":        len(retries_v),
        })
    return resultado


@app.delete("/reset")
def limpiar_metricas():
    """Limpia los datos en memoria entre experimentos. No borra el archivo."""
    global inicio_sistema
    logs_eventos.clear()
    inicio_sistema = time.time()
    return {"status": "limpio"}


@app.get("/health")
def health():
    return {"status": "ok", "eventos_registrados": len(logs_eventos)}