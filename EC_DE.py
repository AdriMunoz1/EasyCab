import threading
import sys
import time
from kafka import KafkaProducer, KafkaConsumer
import socket

# Variables globales
taxi_status_lock = threading.Lock()
taxi_status = "en movimiento"
taxi_position = (1, 1)
destination_position = None
taxi_thread = None
producer = None
central_socket = None


def get_parameters():
    if len(sys.argv) != 8:
        print("Uso: python EC_DE.py <IP_Central> <Port_Central> <IP_Broker> <Port_Broker> <IP_S> <Port_S> <ID_Taxi>")
        sys.exit(1)

    ip_central = sys.argv[1]
    port_central = int(sys.argv[2])
    ip_broker = sys.argv[3]
    port_broker = int(sys.argv[4])
    ip_s = sys.argv[5]
    port_s = int(sys.argv[6])
    id_taxi = int(sys.argv[7])

    return ip_central, port_central, ip_broker, port_broker, ip_s, port_s, id_taxi


def send_message(socket, message):
    try:
        socket.send(message.encode("utf-8"))
    except Exception as e:
        print(f"Error al enviar mensaje: {e}")


def receive_message(socket):
    try:
        return socket.recv(1024).decode("utf-8")
    except Exception as e:
        print(f"Error al recibir mensaje: {e}")
        return ""


def validate_taxi(central_ip, central_port, taxi_id):
    global central_socket
    try:
        # Conectar con el servidor central usando la IP y el puerto especificados
        central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        central_socket.connect((central_ip, central_port))
        send_message(central_socket, f"AUTH#{taxi_id}")
        response = receive_message(central_socket)
        print(f"Respuesta de EC_Central: {response}")
        return response == "OK"
    except Exception as e:
        print(f"Error al conectar con EC_Central: {e}")
        return False


def send_taxi_position():
    message = f"POSITION#{taxi_position}"
    producer.send('taxis_updates', message.encode('utf-8'))


def move_taxi_to_destination(destination):
    global taxi_position
    while taxi_position != destination:
        with taxi_status_lock:
            if taxi_status == "detenido":
                print(f"Taxi detenido en {taxi_position}")
                return
        taxi_position = update_position(taxi_position, destination)
        time.sleep(1)
        send_taxi_position()
        print(f"Taxi moviéndose a {taxi_position}")


def update_position(current, destination):
    x, y = current

    if x < destination[0]:
        x += 1
    elif x > destination[0]:
        x -= 1

    if y < destination[1]:
        y += 1
    elif y > destination[1]:
        y -= 1

    return (x, y)


def handle_central_commands(consumer):
    global destination_position
    for message in consumer:
        command = message.value.decode('utf-8')
        print(f"Comando recibido: {command}")
        if command.startswith("STOP#"):
            with taxi_status_lock:
                taxi_status = "detenido"
            send_message(central_socket, "OK")
        elif command.startswith("RESUME#"):
            with taxi_status_lock:
                taxi_status = "en movimiento"
            send_message(central_socket, "OK")
            if taxi_thread and not taxi_thread.is_alive():
                taxi_thread = threading.Thread(target=move_taxi_to_destination, args=(destination_position,))
                taxi_thread.start()
        elif command.startswith("DESTINATION#"):
            destination_position = eval(command.split("#")[2])
            print(f"Nuevo destino: {destination_position}")
            if taxi_thread and taxi_thread.is_alive():
                print("Taxi en movimiento, esperando a que termine.")
            else:
                taxi_thread = threading.Thread(target=move_taxi_to_destination, args=(destination_position,))
                taxi_thread.start()


def main():
    global producer
    ip_central, port_central, ip_broker, port_broker, ip_s, port_s, id_taxi = get_parameters()

    # Configurar Kafka Producer para enviar actualizaciones de posición
    producer = KafkaProducer(bootstrap_servers=f'{ip_broker}:{port_broker}')

    # Validar el taxi con el servidor central usando la IP y el puerto proporcionados
    if validate_taxi(ip_central, port_central, id_taxi):
        # Configurar Kafka Consumer para recibir comandos de la central
        consumer = KafkaConsumer(
            'taxis_topic',
            bootstrap_servers=f'{ip_broker}:{port_broker}',
            group_id=f"grupo_taxi_{id_taxi}",
            auto_offset_reset='earliest'
        )

        # Iniciar un hilo para manejar comandos de la central
        threading.Thread(target=handle_central_commands, args=(consumer,)).start()
    else:
        print("Autenticación fallida.")
        if central_socket:
            central_socket.close()


if __name__ == "__main__":
    main()
