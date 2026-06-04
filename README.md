# Plataforma Distribuida de Consultas Geoespaciales con Apache Kafka

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-009688.svg)](https://fastapi.tiangolo.com/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-7.5.0-black.svg)](https://kafka.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)
[![Redis](https://img.shields.io/badge/Redis-Alpine-DC382D.svg)](https://redis.io/)

Este proyecto implementa una arquitectura distribuida y asíncrona guiada por eventos (EDA) para procesar consultas analíticas sobre edificaciones en la Región Metropolitana de Santiago utilizando el dataset Google Open Buildings. 

El sistema está diseñado para manejar alta concurrencia, soportar spikes de tráfico y garantizar la tolerancia a fallos mediante mecanismos de reintentos exponenciales y Dead Letter Queues (DLQ) gestionados por Apache Kafka.

---

## Arquitectura del Sistema

El ecosistema está completamente dockerizado y se compone de microservicios independientes que interactúan mediante el paso de mensajes:

### Infraestructura Base
* **Apache Kafka & Zookeeper:** Broker de mensajería central que desacopla la emisión de consultas del procesamiento en el backend.
* **Redis:** Caché en memoria configurado con política allkeys-lru y límite de 500MB para optimizar el rendimiento ante solicitudes recurrentes (Hit/Miss).

### Microservicios (FastAPI)
* generador_trafico: Simula clientes de logística publicando ráfagas de consultas (Q1-Q5) en Kafka bajo distribuciones estadísticas (Uniforme o Zipf).
* sistema_cache: Interfaz REST que verifica la existencia de respuestas pre-calculadas en Redis.
* generador_respuestas: Motor de cálculo que carga el dataset en RAM y procesa algoritmos espaciales. Simula caídas y fallos controlados para pruebas de estrés.
* consumer: Worker escalable que lee el tópico principal, orquesta la consulta y gestiona las fallas derivando mensajes a colas de reintento.
* retry_consumer: Servicio especializado que implementa Exponential Backoff para recuperar fallos temporales previniendo el efecto thundering herd.
* sistema_metricas: Observabilidad centralizada. Registra latencias (p50/p95), throughput (QPS), retry rates y recovery times, persistiendo datos en formato .jsonl.

### Topología de Tópicos
* queries (4 particiones): Cola principal para balanceo de carga.
* queries-retry: Cola temporal de esperas exponenciales.
* queries-dlq: Dead Letter Queue para consultas descartadas definitivamente (límite de 3 reintentos).

---

## Requisitos e Instalación

1. Tener instalado Docker y Docker Compose.
2. Descargar el archivo open_buildings_rm.csv y colocarlo dentro de la carpeta /data. (Nota: si no se provee, el sistema simulará los datos automáticamente).

### Levantar la arquitectura
Para construir las imágenes e iniciar todos los servicios en segundo plano:
```bash
docker compose up -d --build
```
Para verificar que el ecosistema está saludable y Kafka levantó correctamente:
```bash
docker compose ps
```

---

## Guía de Experimentos

El proyecto está preparado para probar distintos escenarios de resiliencia y escalabilidad distribuida.

### 1. Escalamiento Horizontal (Throughput)
Aprovechando las 4 particiones del tópico principal, puedes escalar los workers dinámicamente para procesar el tráfico en paralelo:
```bash
# Escalar a 3 consumidores concurrentes
docker compose up -d --scale consumer=3
```

### 2. Simulación de Fallos (Tolerancia y Backoff)
Para evaluar cómo el sistema encola mensajes, aplica tiempos de espera y rescata consultas sin perder datos, puedes inyectar una tasa de fallo artificial (ej. 30%):
1. Edita el archivo docker-compose.yml en el servicio generador_respuestas y cambia FAILURE_RATE: "0.3".
2. Actualiza el servicio en caliente:
   ```bash
   docker compose up -d --no-deps generador_respuestas
   ```

### 3. Observabilidad en Tiempo Real
El sistema expone un panel de métricas consolidado. Abre tu navegador o usa curl:
* **Métricas globales:** http://localhost:8001/stats (Muestra Throughput QPS, Latencias p50/p95, Recovery Rate, etc.)
* **Limpiar telemetría (para nuevos experimentos):**
  ```bash
  curl.exe -X DELETE http://localhost:8001/reset
  ```

---

## Autores

* **Nicolás Canelo Figueroa**
* **Cristóbal Rivera Guzmán**

Escuela de Ingeniería en Informática y Telecomunicaciones  
**Universidad Diego Portales**
