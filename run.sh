#!/bin/bash

# python3 EC_Central.py puerto_central ip_broker puerto_broker
EC_CENTRAL="python3 EC_Central.py 8000 localhost 9092"

# python3 EC_DE.py ip_central puerto_central ip_broker puerto_broker ip_s puerto_s id_taxi
EC_DE="python3 EC_DE.py localhost 8000 localhost 9092 localhost 8500 1"

# python3 EC_DE.py ip_de puerto_de
EC_S="python3 EC_S.py"

# python3 EC_Customer.py ip_broker puerto_broker id_cliente
EC_CUSTOMER="python3 EC_Customer.py"

# Ejecutar EC_Central
echo "Ejecutando EC_Central..."
$EC_CENTRAL &
PID_CENTRAL=$!
echo "EC_Central en ejecución (PID: $PID_CENTRAL). Esperando 5 segundos..."
sleep 5

# Ejecutar EC_DE
echo "Ejecutando EC_DE..."
$EC_DE &
PID_DE=$!
echo "EC_DE en ejecución (PID: $PID_DE). Esperando 5 segundos..."
sleep 5

# Si no tiene permisos:
# chmod +x run.sh