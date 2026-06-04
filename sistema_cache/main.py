from fastapi import FastAPI
import redis
import httpx
import json
import time
import os

app = FastAPI()

cache = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis_cache"),
    port=6379, db=0, decode_responses=True
)

URL_RESPUESTAS = os.getenv("URL_RESPUESTAS", "http://generador_respuestas:8000")
URL_METRICAS   = os.getenv("URL_METRICAS",   "http://sistema_metricas:8000")
TTL_DEFAULT    = int(os.getenv("TTL_SEGUNDOS", "60"))

# ── helpers ──────────────────────────────────────────────────────────────────

async def registrar_metrica(tipo: str, consulta: str, latencia_ms: float):
    evento = {"tipo": tipo, "consulta": consulta, "latencia_ms": latencia_ms}
    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{URL_METRICAS}/log", json=evento)
        except Exception as e:
            print(f"Error enviando métrica: {e}")

async def resolver_consulta(cache_key: str, url_backend: str, consulta_tipo: str):
    """
    Lógica genérica hit/miss reutilizada por todos los endpoints.
    Siempre devuelve {"cache_hit": bool, "data": ..., "error": bool}
    """
    start = time.time()

    # ── HIT ──
    cached = cache.get(cache_key)
    if cached:
        latencia = (time.time() - start) * 1000
        await registrar_metrica("hit", consulta_tipo, latencia)
        return {"cache_hit": True, "error": False, "data": json.loads(cached)}

    # ── MISS ──
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(url_backend, timeout=5.0)
            resp.raise_for_status()
            datos = resp.json()
        except Exception as exc:
            # No explotar: devolver error para que el consumer decida
            latencia = (time.time() - start) * 1000
            await registrar_metrica("error", consulta_tipo, latencia)
            return {"cache_hit": False, "error": True, "detail": str(exc)}

    cache.setex(cache_key, TTL_DEFAULT, json.dumps(datos))
    latencia = (time.time() - start) * 1000
    await registrar_metrica("miss", consulta_tipo, latencia)
    return {"cache_hit": False, "error": False, "data": datos}

# ── endpoints ────────────────────────────────────────────────────────────────

@app.get("/q1/{zone_id}")
async def proxy_q1(zone_id: str, confidence_min: float = 0.0):
    return await resolver_consulta(
        cache_key   = f"count:{zone_id}:conf={confidence_min}",
        url_backend = f"{URL_RESPUESTAS}/q1/{zone_id}?confidence_min={confidence_min}",
        consulta_tipo = "Q1"
    )

@app.get("/q2/{zone_id}")
async def proxy_q2(zone_id: str, confidence_min: float = 0.0):
    return await resolver_consulta(
        cache_key   = f"area:{zone_id}:conf={confidence_min}",
        url_backend = f"{URL_RESPUESTAS}/q2/{zone_id}?confidence_min={confidence_min}",
        consulta_tipo = "Q2"
    )

@app.get("/q3/{zone_id}")
async def proxy_q3(zone_id: str, confidence_min: float = 0.0):
    return await resolver_consulta(
        cache_key   = f"density:{zone_id}:conf={confidence_min}",
        url_backend = f"{URL_RESPUESTAS}/q3/{zone_id}?confidence_min={confidence_min}",
        consulta_tipo = "Q3"
    )

@app.get("/q4/{zone_a}/{zone_b}")
async def proxy_q4(zone_a: str, zone_b: str, confidence_min: float = 0.0):
    return await resolver_consulta(
        cache_key   = f"compare:density:{zone_a}:{zone_b}:conf={confidence_min}",
        url_backend = f"{URL_RESPUESTAS}/q4/{zone_a}/{zone_b}?confidence_min={confidence_min}",
        consulta_tipo = "Q4"
    )

@app.get("/q5/{zone_id}")
async def proxy_q5(zone_id: str, bins: int = 5):
    return await resolver_consulta(
        cache_key   = f"confidence_dist:{zone_id}:bins={bins}",
        url_backend = f"{URL_RESPUESTAS}/q5/{zone_id}?bins={bins}",
        consulta_tipo = "Q5"
    )

@app.get("/health")
def health():
    return {"status": "ok"}