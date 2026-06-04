from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
import os
import httpx
import asyncio

# ── Configuración ─────────────────────────────────────────────────────────────

KAFKA_BROKER    = os.getenv("KAFKA_BROKER",    "kafka:9092")
TOPIC_PRINCIPAL = os.getenv("TOPIC_PRINCIPAL", "queries")
TOPIC_RETRY     = os.getenv("TOPIC_RETRY",     "queries-retry")
TOPIC_DLQ       = os.getenv("TOPIC_DLQ",       "queries-dlq")
GROUP_ID        = os.getenv("GROUP_ID",         "processors")
MAX_REINTENTOS  = int(os.getenv("MAX_REINTENTOS", "3"))
URL_CACHE       = os.getenv("URL_CACHE",        "http://sistema_cache:8000")
URL_METRICAS    = os.getenv("URL_METRICAS",     "http://sistema_metricas:8000")

# ── Conexión a Kafka ──────────────────────────────────────────────────────────

def crear_consumer():
    for intento in range(10):
        try:
            consumer = KafkaConsumer(
                TOPIC_PRINCIPAL,
                bootstrap_servers=KAFKA_BROKER,
                group_id=GROUP_ID,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True
            )
            print(f"Consumer conectado a Kafka (grupo: {GROUP_ID})")
            return consumer
        except NoBrokersAvailable:
            print(f"Kafka no disponible, reintentando ({intento+1}/10)...")
            time.sleep(3)
    raise Exception("No se pudo conectar a Kafka")

def crear_producer():
    for intento in range(10):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
        except NoBrokersAvailable:
            print(f"Kafka Producer no disponible, reintentando ({intento+1}/10)...")
            time.sleep(3)
    raise Exception("No se pudo crear el Producer")

# ── Construcción de URL según tipo de consulta ────────────────────────────────

def construir_url(mensaje: dict) -> str:
    """Arma la URL del caché según el tipo de consulta y sus parámetros."""
    tipo   = mensaje["query_type"]
    zona   = mensaje["zone_id"]
    params = mensaje.get("params", {})
    conf   = params.get("confidence_min", 0.0)

    if tipo == "Q1":
        return f"{URL_CACHE}/q1/{zona}?confidence_min={conf}"
    elif tipo == "Q2":
        return f"{URL_CACHE}/q2/{zona}?confidence_min={conf}"
    elif tipo == "Q3":
        return f"{URL_CACHE}/q3/{zona}?confidence_min={conf}"
    elif tipo == "Q4":
        zona_b = params.get("zone_b", zona)
        return f"{URL_CACHE}/q4/{zona}/{zona_b}?confidence_min={conf}"
    elif tipo == "Q5":
        bins = params.get("bins", 5)
        return f"{URL_CACHE}/q5/{zona}?bins={bins}"
    else:
        raise ValueError(f"Tipo de consulta desconocido: {tipo}")

# ── Registro de métricas ──────────────────────────────────────────────────────

def registrar_metrica(tipo: str, mensaje: dict, latencia_ms: float, extra: dict = {}):
    """Envía métrica al sistema de métricas vía HTTP (síncrono desde el consumer)."""
    evento = {
        "tipo":        tipo,
        "consulta":    mensaje["query_type"],
        "zona":        mensaje["zone_id"],
        "latencia_ms": latencia_ms,
        "msg_id":      mensaje["id"],
        "retry_count": mensaje.get("retry_count", 0),
        **extra
    }
    try:
        httpx.post(f"{URL_METRICAS}/log", json=evento, timeout=2.0)
    except Exception as e:
        print(f"Error enviando métrica: {e}")

# ── Manejo de fallos ──────────────────────────────────────────────────────────

def manejar_fallo(mensaje: dict, producer: KafkaProducer, detalle: str):
    """
    Decide si reenviar al tópico de retry o mandar a la DLQ
    según el número de reintentos acumulados.
    """
    mensaje["retry_count"] += 1

    if mensaje["retry_count"] >= MAX_REINTENTOS:
        # Superó el límite → DLQ
        producer.send(TOPIC_DLQ, value=mensaje)
        print(f"[DLQ] id={mensaje['id'][:8]} | motivo={detalle}")
        registrar_metrica("dlq", mensaje, 0, {"detalle": detalle})
    else:
        # Backoff exponencial: espera 2^retry segundos antes de reintentar
        backoff = 2 ** mensaje["retry_count"]
        mensaje["proceso_despues_de"] = time.time() + backoff
        producer.send(TOPIC_RETRY, value=mensaje)
        print(f"[RETRY {mensaje['retry_count']}/{MAX_REINTENTOS}] "
              f"id={mensaje['id'][:8]} | backoff={backoff}s")
        registrar_metrica("retry", mensaje, 0, {"backoff_seg": backoff})

# ── Loop principal ────────────────────────────────────────────────────────────

def procesar_mensaje(mensaje: dict, producer: KafkaProducer):
    """Procesa un mensaje: llama al caché y maneja el resultado."""
    start = time.time()

    try:
        url = construir_url(mensaje)
    except ValueError as e:
        print(f"Mensaje inválido, descartando: {e}")
        return

    try:
        respuesta = httpx.get(url, timeout=5.0)
        respuesta.raise_for_status()
        datos = respuesta.json()
    except Exception as exc:
        # El caché o el generador de respuestas falló
        latencia = (time.time() - start) * 1000
        manejar_fallo(mensaje, producer, str(exc))
        return

    latencia = (time.time() - start) * 1000

    # El caché nos informa si fue hit o miss (gracias al cambio que hicimos)
    if datos.get("error"):
        manejar_fallo(mensaje, producer, datos.get("detail", "error desconocido"))
        return

    tipo_evento = "hit" if datos.get("cache_hit") else "miss"
    registrar_metrica(tipo_evento, mensaje, latencia)
    print(f"[{tipo_evento.upper()}] {mensaje['query_type']} | "
          f"zona={mensaje['zone_id']} | {latencia:.1f}ms")


if __name__ == "__main__":
    print("Iniciando Consumer...")
    time.sleep(8)  # espera a que Kafka y el caché estén listos

    consumer = crear_consumer()
    producer = crear_producer()

    for msg in consumer:
        procesar_mensaje(msg.value, producer)