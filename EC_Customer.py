#!/usr/bin/env python3

import sys
import uuid
import json
import time
from confluent_kafka import Producer, Consumer


# ==============================
# FUNCIÓN PARA CARGAR DESTINOS
# ==============================

def load_request_data(filename):
    """
    Carga todos los destinos desde un archivo JSON.
    """
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            if "Requests" in data and data["Requests"]:
                return [request["Id"] for request in data["Requests"]]
            else:
                print(f"No hay solicitudes en {filename}.")
                sys.exit(1)
    except FileNotFoundError:
        print(f"Archivo {filename} no encontrado.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error al parsear el archivo {filename}.")
        sys.exit(1)


# ==============================
# FUNCIÓN PARA ENVIAR SOLICITUD
# ==============================

def send_service_request(producer, client_id, request_id, position, destination):
    """
    Envía una solicitud de servicio a Kafka.
    """
    service_request = {
        "RequestId": request_id,
        "ClientId": client_id,
        "Position": position,
        "Destination": destination
    }

    try:
        producer.produce(
            topic='service_requests',
            value=json.dumps(service_request).encode('utf-8')
        )
        producer.flush()
        print(f"[OK] Solicitud enviada -> RequestId={request_id}, Cliente={client_id}, Posición={position}, Destino={destination}")
    except Exception as e:
        print(f"[ERROR] No se pudo enviar la solicitud: {e}")


# ==============================
# FUNCIÓN PARA ESPERAR RESPUESTA
# ==============================

def wait_for_response(consumer, request_id, client_id, position_container):
    """
    Espera respuesta de aceptación o rechazo, y luego, si aceptado,
    espera a que se complete el trayecto.
    """
    accepted = False

    print(f"[INFO] Esperando respuesta para RequestId={request_id} ...")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[ERROR] Consumer error: {msg.error()}")
            continue

        try:
            response = json.loads(msg.value().decode('utf-8'))
        except Exception as e:
            print(f"[ERROR] No se pudo decodificar la respuesta: {e}")
            continue

        # Filtrar solo mensajes para este cliente y request
        if response.get("RequestId") != request_id:
            continue

        status = response.get("Status")
        print(f"[INFO] Respuesta recibida: Status={status}")

        if status == "accepted":
            accepted = True
            print(f"[OK] Solicitud aceptada. Esperando a que se complete el trayecto...")
        elif status == "denied":
            print(f"[INFO] Solicitud denegada. Se intentará el siguiente destino tras 4 segundos.")
            return "denied"
        elif status == "completed":
            if accepted:
                new_pos = response.get("NewPosition")
                if new_pos:
                    position_container[0] = new_pos
                    print(f"[INFO] Nueva posición del cliente: {position_container[0]}")
                else:
                    print("[WARN] No se recibió NewPosition, se mantiene posición anterior.")
                return "completed"
            else:
                print(f"[WARN] Recibido 'completed' sin 'accepted'. Ignorado.")
        else:
            print(f"[WARN] Estado desconocido: {status}")


# ==============================
# PROGRAMA PRINCIPAL
# ==============================

def main():
    if len(sys.argv) != 6:
        print("Uso: python3 EC_Customer.py <broker_ip:port> <client_id> <x> <y> <request_file.json>")
        sys.exit(1)

    broker_ip_port = sys.argv[1]
    client_id = sys.argv[2]
    x = sys.argv[3]
    y = sys.argv[4]
    request_file = sys.argv[5]

    position = f"{x},{y}"
    position_container = [f"{x},{y}"]

    destinos = load_request_data(request_file)

    # Configurar Producer
    producer_conf = {'bootstrap.servers': broker_ip_port}
    producer = Producer(producer_conf)

    # Configurar Consumer
    consumer_conf = {
        'bootstrap.servers': broker_ip_port,
        'group.id': f"ec_customer_{uuid.uuid4()}",
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe(['service_responses'])

    request_counter = 1

    for destino in destinos:
        # Generar ID legible: <client_id>-<destino>-<num>
        request_id = f"{client_id}-{destino}-{uuid.uuid4().hex[:6]}"
        request_counter += 1

        send_service_request(producer, client_id, request_id, position_container[0], destino)

        # Esperar respuesta y actuar en consecuencia
        result = wait_for_response(consumer, request_id, client_id, position_container)

        if result == "denied":
            time.sleep(4)  # Esperar antes de solicitar siguiente
        elif result == "completed":
            print(f"[INFO] Esperando 4 segundos antes de solicitar el siguiente destino...")
            time.sleep(4)
        else:
            print(f"[WARN] Estado inesperado: {result}. Continuando al siguiente destino.")

    consumer.close()
    print("[INFO] Todas las solicitudes procesadas.")


if __name__ == "__main__":
    main()
