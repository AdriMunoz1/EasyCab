#!/bin/bash

# Ruta a la carpeta de instalación de Kafka
KAFKA_HOME="../kafka_2.13-3.8.0"

# Detener Kafka
echo "Deteniendo Kafka..."
$KAFKA_HOME/bin/kafka-server-stop.sh
sleep 5  # Espera para asegurarse de que Kafka se detenga

# Detener Zookeeper
echo "Deteniendo Zookeeper..."
$KAFKA_HOME/bin/zookeeper-server-stop.sh

echo "Kafka ha sido detenido."
