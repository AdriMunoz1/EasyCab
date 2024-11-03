import socket
import sqlite3
import threading
import json
from kafka import KafkaProducer, KafkaConsumer

# Configuración de Kafka
KAFKA_SERVER = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Tamaño del mapa de la ciudad
size = 20

# Cargar el mapa de la ciudad
def load_city_map(filename):
    city_map = [['' for _ in range(size)] for _ in range(size)]
    try:
        with open(filename, 'r') as file:
            for line in file:
                parts = line.strip().split()
                if len(parts) == 3:
                    id_localizacion = parts[0]
                    coord_x = int(parts[1])
                    coord_y = int(parts[2])
                    city_map[coord_x][coord_y] = id_localizacion
        print("Mapa de la ciudad cargado correctamente.")
    except Exception as e:
        print(f"Error al cargar el mapa de la ciudad: {e}")
    return city_map

# Actualizar el estado del taxi en la base de datos
def update_taxi_status(status, id_taxi):
    try:
        connection = sqlite3.connect('taxis.db')
        cursor = connection.cursor()
        cursor.execute('''
        UPDATE taxis SET estado = ? WHERE id = ?
        ''', (status, id_taxi))
        connection.commit()
        connection.close()
        print(f"[Base de Datos] Taxi {id_taxi} actualizado a estado '{status}'.")
    except sqlite3.Error as e:
        print(f"[Error BD] Error al actualizar el estado del taxi {id_taxi}: {e}")

# Función para autenticar el taxi en la BD
def authenticate_taxi(id_taxi):
    try:
        connection = sqlite3.connect('taxis.db')
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM taxis WHERE id = ?', (id_taxi,))
        taxi = cursor.fetchone()
        connection.close()

        if taxi:
            print(f"\n[Autenticación] Taxi {id_taxi} autenticado correctamente.")
            # Actualizar el estado del taxi a 'OK' en la base de datos
            update_taxi_status("OK", int(id_taxi))
            return "OK"
        else:
            print(f"[Autenticación] Taxi {id_taxi} no está registrado.")
            return "DENIED"
    except sqlite3.Error as e:
        print(f"[Error BD] Error al autenticar el taxi {id_taxi}: {e}")
        return "ERROR"

# Hilo para manejar solicitudes de autenticación de taxis por sockets
def handle_auth_requests_sockets(ip, port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((ip, port))
    server_socket.listen()

    print(f"\n[Autenticación] Servidor de autenticación escuchando en {ip}:{port}")

    while True:
        client_socket, address = server_socket.accept()
        print(f"[Autenticación] Conexión aceptada desde {address}")

        try:
            # Recibir solicitud de autenticación
            data = client_socket.recv(1024).decode('utf-8')
            if data.startswith("AUTH#"):
                taxi_id = data.split("#")[1]
                auth_response = authenticate_taxi(taxi_id)
                client_socket.send(auth_response.encode('utf-8'))
        except Exception as e:
            print(f"[Error] Error al manejar solicitud de autenticación: {e}")
        finally:
            client_socket.close()

# Hilo para manejar los comandos del usuario
def command_input_handler():
    while True:
        try:
            command = input("Introduce un comando (STOP/RESUME/DESTINATION/RETURN) para el taxi: ")
            taxi_id = input("Introduce el ID del taxi: ")

            if command in ["STOP", "RESUME"]:
                send_command(taxi_id, command)
            elif command == "DESTINATION":
                new_destination = input("Introduce la nueva localización (ID_LOCALIZACION): ")
                send_command(taxi_id, "DESTINATION", new_destination)
            elif command == "RETURN":
                send_command(taxi_id, "RETURN")
            else:
                print("Comando no válido. Usa STOP, RESUME, DESTINATION o RETURN.")
        except Exception as e:
            print(f"Error al introducir comandos: {e}")

# Función para enviar comandos al taxi a través de Kafka
def send_command(taxi_id, command, extra_param=None):
    msg = {
        'taxi_id': taxi_id,
        'command': command,
        'extra_param': extra_param
    }
    producer.send('taxi_commands', msg)
    producer.flush()  # Asegurarse de que el mensaje sea enviado inmediatamente
    print(f"Comando {command} enviado al taxi {taxi_id}")

# Ejecutar el servidor EC_Central
if __name__ == "__main__":
    city_map = load_city_map('city_map.txt')

    # Crear hilo para el manejo de comandos de usuario
    command_thread = threading.Thread(target=command_input_handler, daemon=False)
    command_thread.start()

    # Crear hilo para manejar las solicitudes de autenticación por sockets
    auth_thread = threading.Thread(target=handle_auth_requests_sockets, args=('localhost', 7999), daemon=False)
    auth_thread.start()

    # Asegurarse de que el programa no termine
    command_thread.join()
    auth_thread.join()
