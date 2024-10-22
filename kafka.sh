#!/bin/bash

# Ruta relativa a la carpeta de Kafka
KAFKA_HOME="../kafka_2.13-3.8.0"

# Iniciar Zookeeper
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &

# Esperar unos segundos para que Zookeeper arranque
sleep 5

# Iniciar Kafka
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &

# Si no tiene permisos:
# chmod +x kafka.sh