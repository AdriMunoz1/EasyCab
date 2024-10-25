import socket
import threading
import sys
import time

# Variables globales
taxi_status_lock = threading.Lock()  # Lock para el estado del taxi
taxi_status = "en movimiento"  # Estado inicial
taxi_position = (1, 1)  # Posición inicial
destination_position = None  # Posición de destino
taxi_thread = None  # Hilo de imiento
is_moving_thread_running = False  # Variable de control del hilo de movimiento
sensor_socket = None  # Socket del sensor

# Obtener parámetros
def get_parameters():
    if len(sys.argv) <= 5:
        print("Usage: python EC_DE <EC_Central_IP> <EC_Central_Port> <EC_S_IP> <EC_S_Port> <Taxi_ID>")
        sys.exit(1)
    ip_central = sys.argv[1]
    port_central = int(sys.argv[2])
    broker_ip = sys.argv[3]
    broker_port = int(sys.argv[4])
    taxi_id = sys.argv[5]
    return ip_central, port_central, broker_ip, broker_port, taxi_id

# Validar el taxi
def validate_taxi(ip_central, port_central, taxi_id, client_socket):
    try:
        msg = f"AUTH#{taxi_id}"
        client_socket.send(msg.encode("utf-8"))
        response = client_socket.recv(1024).decode("utf-8")
        print(f"Respuesta de EC_Central: {response}")
        if response == "OK":
            print("Taxi authenticated successfully!")
            return True
        else:
            print("Authentication failed")
            return False
    except Exception as e:
        print(f"Error al autenticar el taxi: {e}")
        return False

# Enviar posición a EC_Central
def send_taxi_position_to_central(position, central_socket, taxi_id):
    try:
        message = f"POSITION#{taxi_id}#{position}"
        central_socket.send(message.encode('utf-8'))
    except Exception as e:
        print(f"Error al enviar la posición del taxi {taxi_id}: {e}")

# Iniciar el hilo de movimiento
def start_moving_thread(central_socket, taxi_id):
    global taxi_thread, is_moving_thread_running
    if not is_moving_thread_running:
        taxi_thread = threading.Thread(target=move_taxi_to_destination, args=(destination_position, central_socket, taxi_id))
        taxi_thread.start()
        is_moving_thread_running = True

# Manejador de comandos de EC_Central
def handle_central_commands(central_socket, taxi_id):
    global taxi_status, destination_position, sensor_socket
    try:
        while True:
            message = central_socket.recv(1024).decode("utf-8")
            if message.startswith("STOP#"):
                with taxi_status_lock:
                    print("Recibido comando para detener el taxi")
                    taxi_status = "detenido"
                central_socket.send("OK".encode('UTF-8'))
            elif message.startswith("RESUME#"):
                with taxi_status_lock:
                    print("Recibido comando para reanudar el taxi")
                    taxi_status = "en movimiento"
                central_socket.send("OK".encode('UTF-8'))
                if sensor_socket:
                    sensor_socket.send(b'RESUME')
                start_moving_thread(central_socket, taxi_id)
            elif message.startswith("DESTINATION#"):
                parts = message.split("#")
                destination_position = eval(parts[2])
                print(f"Recibido nuevo destino: {destination_position}")
                with taxi_status_lock:
                    taxi_status = "en movimiento"
                start_moving_thread(central_socket, taxi_id)
            elif message.startswith("RETURN#"):
                print("Recibido comando para volver a la base [1,1]")
                destination_position = (1, 1)
                with taxi_status_lock:
                    taxi_status = "en movimiento"
                start_moving_thread(central_socket, taxi_id)
    except Exception as e:
        print(f"Error al recibir comandos: {e}")

# Mover taxi
def move_taxi_to_destination(destination_position, central_socket, taxi_id):
    global taxi_status, taxi_position, is_moving_thread_running
    if taxi_position is None or destination_position is None:
        print("Error: Posiciones no inicializadas correctamente.")
        is_moving_thread_running = False
        return

    while taxi_position != destination_position:
        with taxi_status_lock:
            if taxi_status == "detenido":
                print(f"Taxi {taxi_id} detenido en {taxi_position}")
                is_moving_thread_running = False
                return

        # Actualizar posición en eje X
        if taxi_position[0] < destination_position[0]:
            taxi_position = (taxi_position[0] + 1, taxi_position[1])
        elif taxi_position[0] > destination_position[0]:
            taxi_position = (taxi_position[0] - 1, taxi_position[1])

        # Actualizar posición en eje Y
        if taxi_position[1] < destination_position[1]:
            taxi_position = (taxi_position[0], taxi_position[1] + 1)
        elif taxi_position[1] > destination_position[1]:
            taxi_position = (taxi_position[0], taxi_position[1] - 1)

        time.sleep(1)
        send_taxi_position_to_central(taxi_position, central_socket, taxi_id)
        print(f"Taxi {taxi_id} moviéndose a {taxi_position}")

    if taxi_position == destination_position:
        print(f"Taxi {taxi_id} ha llegado a su destino: {destination_position}")
    is_moving_thread_running = False

# Manejar señales de EC_S
def handle_sensor_signals(client_socket, addr, central_socket, taxi_id):
    global taxi_status, sensor_socket
    try:
        sensor_socket = client_socket
        while True:
            signal = client_socket.recv(1024).decode('utf-8')
            if signal == "KO":
                print("Sensor envió KO, notificando a EC_Central para detener el taxi")
                msg = f"STOP#{taxi_id}"
                central_socket.send(msg.encode("utf-8"))
                with taxi_status_lock:
                    taxi_status = "detenido"
                time.sleep(5)
                msg = f"RESUME#{taxi_id}"
                central_socket.send(msg.encode("utf-8"))
                with taxi_status_lock:
                    taxi_status = "en movimiento"
                start_moving_thread(central_socket, taxi_id)
    except Exception as e:
        print(f"Error manejando la señal del sensor {addr}: {e}")
    finally:
        client_socket.close()

# Escuchar señales de EC_S
def run_sensor_server(taxi_id):
    server_ip = "localhost"
    port = 8001 + int(taxi_id)
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((server_ip, port))
        server.listen()
        print(f"Servidor de sensores del taxi {taxi_id} escuchando en {server_ip}:{port}")

        while True:
            client_socket, addr = server.accept()
            print(f"Conexión aceptada de {addr[0]}:{addr[1]}")
            thread = threading.Thread(target=handle_sensor_signals, args=(client_socket, addr, central_socket, taxi_id))
            thread.start()
    except Exception as e:
        print(f"Error en el servidor del taxi: {e}")
    finally:
        server.close()

# Función principal
def main():
    global central_socket
    ip_central, port_central, broker_ip, broker_port, taxi_id = get_parameters()
    central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    central_socket.connect((ip_central, port_central))

    if validate_taxi(ip_central, port_central, taxi_id, central_socket):
        server_thread = threading.Thread(target=run_sensor_server, args=(taxi_id,))
        server_thread.start()
        central_command_thread = threading.Thread(target=handle_central_commands, args=(central_socket, taxi_id))
        central_command_thread.start()
    else:
        print("Autenticación fallida.")
        central_socket.close()

if __name__ == "__main__":
    main()
