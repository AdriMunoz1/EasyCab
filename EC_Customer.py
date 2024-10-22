import socket
import sys
import time
from kafka import KafkaProducer

def get_parameters():
    if len(sys.argv) <= 3:
        print(f"Error: <IP Broker> <Port Broker>  <ID Cliente>")
        sys.exit(1)

    ip_broker = sys.argv[1]
    port_broker = int(sys.argv[2])
    id_cliente = sys.argv[3]

    return ip_broker, port_broker, id_cliente


def request_taxi(central_socket, id_cliente):
    ubicacion_recogida = input("Introduce la ubicación de recogida:")
    destino = input("Introduce el destino:")
    mensaje = f"REQUEST#{id_cliente}#{ubicacion_recogida}#{destino}"
    central_socket.send(mensaje.encode('utf-8'))
    respuesta = central_socket.recv(1024).decode('utf-8')
    print(f"Respuesta de EC_Central: {respuesta}")


def main():
    ip_broker, port_broker, id_cliente = get_parameters()

    producer = KafkaProducer(bootstrap_servers=f'{ip_broker}:{port_broker}')

    # Leer el archivo con los destinos
    with open('destinos.txt', 'r') as file:
        destinos = file.readlines()

    for destino in destinos:
        # Crear mensaje
        mensaje = f"Cliente {id_cliente} solicita taxi al destino {destino.strip()}"
        producer.send('central_topic', mensaje.encode('utf-8'))
        print(f"Enviando: {mensaje}")
        time.sleep(4)  # Esperar 4 segundos antes de enviar el próximo servicio

######################### MAIN #########################
if __name__ == "__main__":
    main()