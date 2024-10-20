import socket
import sys
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
    print(f"Cliente {id_cliente} conecatado a Kafka en {ip_broker}:{port_broker}")

    while True:
        solicitud = input("¿Quiere pedir un taxi? (s/n):").lower()
        if solicitud == 's':
            request_taxi(producer, id_cliente)
        else:
            print("Adios")
            break

######################### MAIN #########################
if __name__ == "__main__":
    main()