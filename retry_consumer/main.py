from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
import os

# ── Configuración ─────────────────────────────────────────────────────────────

KAFKA_BROKER    = os.getenv("KAFKA_BROKER",    "kafka:9092")
TOPIC_RETRY     = os.getenv("TOPIC_RETRY",     "queries-retry")
TOPIC_PRINCIPAL = os.getenv("TOPIC_PRINCIPAL", "queries")
URL_METRICAS    = os.getenv("URL_METRICAS",    "http://sistema_metricas:8000")

# ── Conexión a Kafka ──────────────────────────────────────────────────────────

def crear_consumer():
    for intento in range(10):
        try:
            consumer = KafkaConsumer(
                TOPIC_RETRY,
                bootstrap_servers=KAFKA_BROKER,
                group_id="retry-processors",   # grupo distinto al consumer principal
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True
            )
            print("Retry-consumer conectado a Kafka")
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

# ── Registro de métricas ──────────────────────────────────────────────────────

def registrar_metrica(tipo: str, mensaje: dict):
    import httpx
    evento = {
        "tipo":        tipo,
        "consulta":    mensaje["query_type"],
        "zona":        mensaje["zone_id"],
        "latencia_ms": 0.0,
        "msg_id":      mensaje["id"],
        "retry_count": mensaje.get("retry_count", 0),
    }
    try:
        httpx.post(f"{URL_METRICAS}/log", json=evento, timeout=2.0)
    except Exception as e:
        print(f"Error enviando métrica: {e}")

# ── Lógica principal ──────────────────────────────────────────────────────────

def procesar_retry(mensaje: dict, producer: KafkaProducer):
    """
    Espera el tiempo de backoff y reenvía al tópico principal.
    El backoff es exponencial: 2^retry_count segundos.
    """
    retry_count = mensaje.get("retry_count", 1)
    proceso_despues_de = mensaje.get("proceso_despues_de", 0)

    # Esperar el tiempo de backoff restante
    tiempo_restante = proceso_despues_de - time.time()
    if tiempo_restante > 0:
        print(f"[BACKOFF] id={mensaje['id'][:8]} | "
              f"esperando {tiempo_restante:.1f}s "
              f"(reintento {retry_count})")
        time.sleep(tiempo_restante)

    # Reenviar al tópico principal para que el consumer lo procese de nuevo
    producer.send(TOPIC_PRINCIPAL, value=mensaje)
    producer.flush()

    registrar_metrica("recovery", mensaje)
    print(f"[REENVIADO] id={mensaje['id'][:8]} | "
          f"reintento {retry_count} → tópico '{TOPIC_PRINCIPAL}'")


if __name__ == "__main__":
    print("Iniciando Retry-Consumer...")
    time.sleep(10)  # espera a que Kafka esté listo

    consumer = crear_consumer()
    producer = crear_producer()

    for msg in consumer:
        procesar_retry(msg.value, producer)