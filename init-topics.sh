#!/bin/bash

KAFKA_BROKER="kafka:9092"

echo "Esperando a que Kafka esté disponible..."
until kafka-topics --bootstrap-server $KAFKA_BROKER --list > /dev/null 2>&1; do
  echo "  Kafka no listo, reintentando en 3s..."
  sleep 3
done
echo "Kafka disponible."

# Función para crear tópico solo si no existe
crear_topico() {
  NOMBRE=$1
  PARTICIONES=$2

  if kafka-topics --bootstrap-server $KAFKA_BROKER --list | grep -q "^${NOMBRE}$"; then
    echo "  Tópico '${NOMBRE}' ya existe, omitiendo."
  else
    kafka-topics --create \
      --bootstrap-server $KAFKA_BROKER \
      --topic $NOMBRE \
      --partitions $PARTICIONES \
      --replication-factor 1
    echo "  Tópico '${NOMBRE}' creado con ${PARTICIONES} partición(es)."
  fi
}

# Crear los 3 tópicos
# queries: 4 particiones para soportar hasta 4 consumers en paralelo
crear_topico "queries"       4
crear_topico "queries-retry" 2
crear_topico "queries-dlq"   1

echo ""
echo "Tópicos disponibles:"
kafka-topics --bootstrap-server $KAFKA_BROKER --list

echo ""
echo "Init completado."