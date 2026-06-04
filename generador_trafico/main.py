import numpy as np
import time
import random
import os
import json
import uuid
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Configuración
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_PRINCIPAL = "queries"
ZONAS = ["Z1", "Z2", "Z3", "Z4", "Z5"]
TIPOS_CONSULTA = ["Q1", "Q2", "Q3", "Q4", "Q5"]
DISTRIBUCION_ACTUAL = os.getenv("DISTRIBUCION", "zipf")
TIEMPO_ESPERA_SEG = float(os.getenv("TIEMPO_ESPERA", "0.5"))

def crear_producer():
    """Crea el producer con reintentos por si Kafka no está listo."""
    for intento in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("Conectado a Kafka correctamente")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka no disponible, reintentando ({intento+1}/10)...")
            time.sleep(3)
    raise Exception("No se pudo conectar a Kafka")

def elegir_zona(distribucion):
    """Elige una zona basándose en la distribución seleccionada."""
    if distribucion == "uniforme":
        return random.choice(ZONAS)
    elif distribucion == "zipf":
        rango = np.random.zipf(a=1.5)
        indice = min(rango - 1, len(ZONAS) - 1)
        return ZONAS[indice]

def construir_mensaje(consulta, zona, confianza, bins=None):
    """Construye el mensaje con todos los campos requeridos."""
    params = {"confidence_min": confianza}
    if bins:
        params["bins"] = bins

    return {
        "id": str(uuid.uuid4()),          # identificador único
        "query_type": consulta,
        "zone_id": zona,
        "params": params,
        "retry_count": 0,                 # siempre 0 en el origen
        "created_at": datetime.now().isoformat(),
        "distribucion": DISTRIBUCION_ACTUAL
    }

def generar_peticion(producer):
    """Construye y publica una consulta en Kafka."""
    consulta = random.choice(TIPOS_CONSULTA)
    zona = elegir_zona(DISTRIBUCION_ACTUAL)
    confianza = random.choice([0.0, 0.5, 0.8])

    if consulta == "Q4":
        zona_b = elegir_zona(DISTRIBUCION_ACTUAL)
        mensaje = construir_mensaje(consulta, zona, confianza)
        mensaje["params"]["zone_b"] = zona_b
    elif consulta == "Q5":
        bins = random.choice([5, 10])
        mensaje = construir_mensaje(consulta, zona, confianza, bins)
    else:
        mensaje = construir_mensaje(consulta, zona, confianza)

    producer.send(TOPIC_PRINCIPAL, value=mensaje)
    print(f"[{DISTRIBUCION_ACTUAL}] Publicado {consulta} | zona={zona} | id={mensaje['id'][:8]}...")

if __name__ == "__main__":
    print("Iniciando Generador de Tráfico...")
    time.sleep(5)  # espera inicial para que Kafka levante

    producer = crear_producer()

    while True:
        generar_peticion(producer)
        time.sleep(TIEMPO_ESPERA_SEG)