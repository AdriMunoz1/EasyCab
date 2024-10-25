import socket
import threading
import sys
import time

# Variables globales
taxi_status_lock = threading.Lock()
taxi_status = "en movimiento"
taxi_position = (1, 1)
destination_position = None
taxi_thread = None
sensor_socket = None
central_socket = None

def get_parameters():
    # Comprobación de parámetros de línea de comandos
    if len(sys.argv) != 6:
        print("Uso: python EC_DE <EC_Central_IP> <EC_Central_Port> <EC_S_IP> <EC_S_Port> <Taxi_ID>")
        sys.exit(1)
    return sys.argv[1], int(sys.argv[2]), sys.argv[3], int(sys.argv[4]), sys.argv[5]

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

def validate_taxi(ip_central, port_central, taxi_id):
    send_message(central_socket, f"AUTH#{taxi_id}")
    response = receive_message(central_socket)
    print(f"Respuesta de EC_Central: {response}")
    return response == "OK"

def send_taxi_position(position):
    message = f"POSITION#{taxi_id}#{position}"
    send_message(central_socket, message)

def move_taxi_to_destination(destination):
    global taxi_position
    while taxi_position != destination:
        with taxi_status_lock:
            if taxi_status == "detenido":
                print(f"Taxi {taxi_id} detenido en {taxi_position}")
                return
        taxi_position = update_position(taxi_position, destination)
        time.sleep(1)
        send_taxi_position(taxi_position)
        print(f"Taxi {taxi_id} moviéndose a {taxi_position}")

def update_position(current, destination):
    x, y = current
    if x < destination[0]: x += 1
    elif x > destination[0]: x -= 1
    if y < destination[1]: y += 1
    elif y > destination[1]: y -= 1
    return (x, y)

def handle_central_commands():
    global destination_position
    while True:
        message = receive_message(central_socket)
        print(f"Comando recibido: {message}")
        if message.startswith("STOP#"):
            with taxi_status_lock:
                taxi_status = "detenido"
            send_message(central_socket, "OK")
        elif message.startswith("RESUME#"):
            with taxi_status_lock:
                taxi_status = "en movimiento"
            send_message(central_socket, "OK")
            if taxi_thread and not taxi_thread.is_alive():
                taxi_thread = threading.Thread(target=move_taxi_to_destination, args=(destination_position,))
                taxi_thread.start()
        elif message.startswith("DESTINATION#"):
            destination_position = eval(message.split("#")[2])
            print(f"Nuevo destino: {destination_position}")
            if taxi_thread and taxi_thread.is_alive():
                print("Taxi en movimiento, esperando a que termine.")
            else:
                taxi_thread = threading.Thread(target=move_taxi_to_destination, args=(destination_position,))
                taxi_thread.start()

def main():
    global central_socket, taxi_id
    ip_central, port_central, broker_ip, broker_port, taxi_id = get_parameters()
    central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    central_socket.connect((ip_central, port_central))
    if validate_taxi(ip_central, port_central, taxi_id):
        command_thread = threading.Thread(target=handle_central_commands)
        command_thread.start()
    else:
        print("Autenticación fallida.")
        central_socket.close()

if __name__ == "__main__":
    main()
