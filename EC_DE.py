import socket
import threading
import sys
import time

# Variable global para manejar el estado del taxi
taxi_status_lock = threading.Lock()  # Lock para controlar el acceso a taxi_status
taxi_status = "en movimiento"  # Estado inicial del taxi
taxi_position = (1, 1)  # Posición inicial del taxi
destination_position = None  # Posición de destino
taxi_thread = None  # Hilo para el movimiento del taxi

def send_taxi_position_to_central(position, central_socket, taxi_id):
    try:
        # Enviar la posición actual del taxi a EC_Central
        message = f"POSITION#{taxi_id}#{position}"
        central_socket.send(message.encode('utf-8'))
    except Exception as e:
        print(f"Error al enviar la posición del taxi {taxi_id}: {e}")

# Función para mover el taxi hacia el destino
def move_taxi_to_destination(destination_position, central_socket, taxi_id):
    global taxi_status, taxi_position

    if taxi_position is None or destination_position is None:
        print("Error: Posiciones no inicializadas correctamente.")
        return

    while taxi_position != destination_position:
        # Revisar el estado del taxi en cada iteración para detener si es necesario
        with taxi_status_lock:
            if taxi_status == "detenido":
                print(f"Taxi {taxi_id} detenido en {taxi_position}")
                return  # El taxi se detiene en la posición actual

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

        # Simular el movimiento con un retardo
        time.sleep(1)

        # Enviar la nueva posición al EC_Central
        send_taxi_position_to_central(taxi_position, central_socket, taxi_id)
        print(f"Taxi {taxi_id} moviéndose a {taxi_position}")

    # Si el taxi no fue detenido, ha llegado al destino
    if taxi_position == destination_position:
        print(f"Taxi {taxi_id} ha llegado a su destino: {destination_position}")

# Hilo para manejar el movimiento del taxi
def taxi_movement_thread(central_socket, taxi_id):
    global destination_position
    move_taxi_to_destination(destination_position, central_socket, taxi_id)

# Obtener los parámetros pasados por línea de comandos
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

# Validar el taxi con EC_Central
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

# Manejo de comandos desde EC_Central
def handle_central_commands(central_socket, taxi_id):
    global taxi_status, taxi_position, destination_position, taxi_thread  # Para actualizar el estado del taxi
    try:
        while True:
            message = central_socket.recv(1024).decode("utf-8")
            if message.startswith("STOP#"):
                with taxi_status_lock:
                    print("Recibido comando para detener el taxi")
                    taxi_status = "detenido"  # Actualizar el estado a "detenido"
                central_socket.send("OK".encode('UTF-8'))
            elif message.startswith("RESUME#"):
                with taxi_status_lock:
                    print("Recibido comando para reanudar el taxi")
                    taxi_status = "en movimiento"  # Reanudar el movimiento
                central_socket.send("OK".encode('UTF-8'))
                # Volver a lanzar el hilo de movimiento si está detenido
                if taxi_thread and not taxi_thread.is_alive():
                    taxi_thread = threading.Thread(target=taxi_movement_thread, args=(central_socket, taxi_id))
                    taxi_thread.start()
            elif message.startswith("DESTINATION#"):
                parts = message.split("#")
                destination_position = eval(parts[2])  # Convertir la posición del destino en tupla
                print(f"Recibido nuevo destino: {destination_position}")
                taxi_status = "en movimiento"
                if taxi_thread and taxi_thread.is_alive():
                    print("Taxi ya está en movimiento, esperando a que termine o se detenga.")
                else:
                    taxi_thread = threading.Thread(target=taxi_movement_thread, args=(central_socket, taxi_id))
                    taxi_thread.start()
            elif message.startswith("RETURN#"):
                print("Recibido comando para volver a la base [1,1]")
                destination_position = (1, 1)
                taxi_status = "en movimiento"
                taxi_thread = threading.Thread(target=taxi_movement_thread, args=(central_socket, taxi_id))
                taxi_thread.start()
    except Exception as e:
        print(f"Error al recibir comandos: {e}")

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
            thread = threading.Thread(target=handle_sensor_signals, args=(client_socket, addr))
            thread.start()
    except Exception as e:
        print(f"Error en el servidor del taxi: {e}")
    finally:
        server.close()

# Función principal del taxi
def main():
    ip_central, port_central, broker_ip, broker_port, taxi_id = get_parameters()

    # Establecer la conexión inicial con EC_Central
    central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    central_socket.connect((ip_central, port_central))

    # Validar el taxi con la conexión establecida
    if validate_taxi(ip_central, port_central, taxi_id, central_socket):
        # Ejecutar el servidor del taxi para escuchar señales de EC_S
        server_thread = threading.Thread(target=run_sensor_server, args=(taxi_id,))
        server_thread.start()

        # Iniciar un hilo para manejar comandos de EC_Central
        central_command_thread = threading.Thread(target=handle_central_commands, args=(central_socket, taxi_id))
        central_command_thread.start()

    else:
        print("Autenticación fallida. Por favor, intenta nuevamente.")
        central_socket.close()

########################## MAIN ##########################
if __name__ == "__main__":
    main()
