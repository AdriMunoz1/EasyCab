import socket
import sqlite3
import threading
import json
import time
from kafka import KafkaProducer, KafkaConsumer
from tkinter import Tk, Canvas
import sys
import signal

# Configuración de Kafka
KAFKA_SERVER = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Tamaño del mapa de la ciudad
size = 20

# Crear el mapa de la ciudad usando Tkinter
class CityMap:
    def __init__(self, root, size):
        self.size = size
        self.canvas = Canvas(root, width=500, height=500, bg='white')
        self.canvas.pack()
        self.cell_size = 500 // size
        self.locations = {}
        self.taxis = {}
        self.customers = {}  # Corregido: Inicializar el diccionario para almacenar clientes
        self.draw_grid()

    def draw_grid(self):
        for i in range(1, self.size + 1):
            for j in range(1, self.size + 1):
                x0 = (i - 1) * self.cell_size
                y0 = (j - 1) * self.cell_size
                x1 = x0 + self.cell_size
                y1 = y0 + self.cell_size
                self.canvas.create_rectangle(x0, y0, x1, y1, outline='black')

    def add_location(self, x, y, label):
        x0 = (x - 1) * self.cell_size + self.cell_size // 2
        y0 = (y - 1) * self.cell_size + self.cell_size // 2
        rect_id = self.canvas.create_rectangle(
            (x - 1) * self.cell_size, (y - 1) * self.cell_size,
            x * self.cell_size, y * self.cell_size,
            fill='blue'
        )
        text_id = self.canvas.create_text(x0, y0, text=label, fill='white', font=('Helvetica', 14, 'bold'))
        self.locations[label] = (rect_id, text_id)

    def add_taxi(self, taxi_id, x, y, color):
        x0 = (x - 1) * self.cell_size + self.cell_size // 2
        y0 = (y - 1) * self.cell_size + self.cell_size // 2
        rect_id = self.canvas.create_rectangle(
            (x - 1) * self.cell_size, (y - 1) * self.cell_size,
            x * self.cell_size, y * self.cell_size,
            fill=color
        )
        text_id = self.canvas.create_text(x0, y0, text=str(taxi_id), fill='white', font=('Helvetica', 14, 'bold'))
        self.taxis[taxi_id] = (rect_id, text_id)

    def move_taxi(self, taxi_id, x, y, color):
        if taxi_id in self.taxis:
            rect_id, text_id = self.taxis[taxi_id]
            self.canvas.coords(
                rect_id,
                (x - 1) * self.cell_size, (y - 1) * self.cell_size,
                x * self.cell_size, y * self.cell_size
            )
            self.canvas.coords(
                text_id,
                (x - 1) * self.cell_size + self.cell_size // 2,
                (y - 1) * self.cell_size + self.cell_size // 2
            )
            self.canvas.itemconfig(rect_id, fill=color)

    def add_customer(self, customer_id, x, y):
        """Añadir un cliente al mapa con color amarillo y su ID en minúscula."""
        x0 = (x - 1) * self.cell_size + self.cell_size // 2
        y0 = (y - 1) * self.cell_size + self.cell_size // 2
        rect_id = self.canvas.create_rectangle(
            (x - 1) * self.cell_size, (y - 1) * self.cell_size,
            x * self.cell_size, y * self.cell_size,
            fill='yellow'
        )
        text_id = self.canvas.create_text(x0, y0, text=str(customer_id).lower(), fill='black', font=('Helvetica', 14, 'bold'))
        self.customers[customer_id] = (rect_id, text_id)
"""
# Cargar el mapa de la ciudad
def load_city_map(city_map):
    try:
        city_map.add_location(3, 5, 'A')
        city_map.add_location(11, 2, 'B')
        city_map.add_location(16, 8, 'C')
        city_map.add_location(16, 17, 'D')
        city_map.add_location(10, 19, 'E')
        print("Mapa de la ciudad cargado correctamente.")
    except Exception as e:
        print(f"Error al cargar el mapa de la ciudad: {e}")
"""
# Cargar el mapa de la ciudad
def load_city_map(city_map):
    try:
        with open('city_map.txt', 'r') as file:
            for line in file:
                parts = line.strip().split()
                if len(parts) == 3:
                    id_localizacion = parts[0]
                    coord_x = int(parts[1])
                    coord_y = int(parts[2])
                    city_map.add_location(coord_x, coord_y, id_localizacion)
        print("Mapa de la ciudad cargado correctamente.")
    except Exception as e:
        print(f"Error al cargar el mapa de la ciudad: {e}")
# Manejar la desconexión del taxi
def handle_exit_signal(signal_received, frame):
    print(f"\n[Desconexión] Cerrando EC_Central...")
    sys.exit(0)

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

# Hilo para manejar los comandos del usuario
def command_input_handler():
    while True:
        try:
            command = input("Introduce un comando (STOP/RESUME/DESTINATION/RETURN) para el taxi: ")
            taxi_id = input("Introduce el ID del taxi: ")

            if command in ["STOP", "RESUME"]:
                send_command(taxi_id, command)
            elif command == "DESTINATION":
                new_destination = input("Introduce la nueva localización (en formato (x, y)): ")
                send_command(taxi_id, "DESTINATION", new_destination)
            elif command == "RETURN":
                send_command(taxi_id, "RETURN")
            else:
                print("Comando no válido. Usa STOP, RESUME, DESTINATION o RETURN.")
        except Exception as e:
            print(f"Error al introducir comandos: {e}")

# Función para manejar actualizaciones del taxi a través de Kafka
def handle_taxi_updates(city_map):
    consumer = KafkaConsumer(
        'taxi_updates',
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )

    try:
        for message in consumer:
            taxi_id = message.value.get('taxi_id')
            status = message.value.get('status')
            position = message.value.get('position')

            if status == 'moving':
                city_map.move_taxi(taxi_id, position[0], position[1], 'green')
            elif status == 'stopped':
                city_map.move_taxi(taxi_id, position[0], position[1], 'red')
            elif status == 'available':
                # Añadir taxi al pool de disponibles
                add_taxi_to_available_pool(taxi_id)
                print(f"Taxi {taxi_id} está ahora disponible en el pool")
    except Exception as e:
        print(f"Error al manejar actualizaciones del taxi: {e}")

# Hilo para manejar solicitudes de autenticación de taxis por sockets
def handle_auth_requests_sockets(ip, port, city_map):
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
                if auth_response == "OK":
                    # Añadir taxi al mapa en la posición inicial (1, 1) en rojo
                    city_map.add_taxi(taxi_id, 1, 1, 'red')
        except Exception as e:
            print(f"[Error] Error al manejar solicitud de autenticación: {e}")
        finally:
            client_socket.close()

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
            return "OK"
        else:
            print(f"[Autenticación] Taxi {id_taxi} no está registrado.")
            return "DENIED"
    except sqlite3.Error as e:
        print(f"[Error BD] Error al autenticar el taxi {id_taxi}: {e}")
        return "ERROR"

TAXIS_DISPONIBLES = []

# Función para manejar solicitudes de clientes a través de Kafka
def handle_customer_requests():
    consumer = KafkaConsumer(
        'central_requests',
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',  # Leer solo los mensajes nuevos
        group_id='ec_central_group'  # Identificador único para evitar leer mensajes antiguos
    )

    try:
        for message in consumer:
            data = message.value
            if data['type'] == 'DESTINATION':
                client_id = data['client_id']
                initial_position = [int(data['initial_position'][0]), int(data['initial_position'][1])]
                destination = (int(data['destination'][0]), int(data['destination'][1]))

                # Añadir el cliente al mapa en la posición inicial
                city_map.add_customer(client_id, initial_position[0], initial_position[1])

                # Asignar taxi disponible
                if TAXIS_DISPONIBLES:
                    assigned_taxi = TAXIS_DISPONIBLES.pop(0)
                    send_command(assigned_taxi, "DESTINATION", destination)
                    print(f"Asignando taxi {assigned_taxi} al cliente {client_id} con destino {destination}")

                    # Responder al cliente que se ha asignado un taxi
                    response = {
                        'client_id': client_id,
                        'status': 'OK',
                        'assigned_taxi': assigned_taxi
                    }
                    producer.send(f'customer_{client_id}_response', response)
                    producer.flush()
                    print(f"Respuesta enviada al cliente {client_id}: Taxi {assigned_taxi} asignado.")
                else:
                    # Responder al cliente que no hay taxis disponibles
                    response = {
                        'client_id': client_id,
                        'status': 'KO',
                        'reason': 'No hay taxis disponibles en este momento'
                    }
                    producer.send(f'customer_{client_id}_response', response)
                    producer.flush()
                    print(f"No hay taxis disponibles para el cliente {client_id}")
    except Exception as e:
        print(f"Error al manejar solicitudes de clientes: {e}")


def add_taxi_to_available_pool(taxi_id):
    if taxi_id not in TAXIS_DISPONIBLES:
        TAXIS_DISPONIBLES.append(taxi_id)
        print(f"Taxi {taxi_id} añadido al pool de disponibles")

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
            # Añadir el taxi al pool de disponibles
            add_taxi_to_available_pool(id_taxi)
            return "OK"
        else:
            print(f"[Autenticación] Taxi {id_taxi} no está registrado.")
            return "DENIED"
    except sqlite3.Error as e:
        print(f"[Error BD] Error al autenticar el taxi {id_taxi}: {e}")
        return "ERROR"
# Ejecutar el servidor EC_Central
if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_exit_signal)  # Capturar la señal Ctrl+C para manejar la desconexión

    root = Tk()
    root.title("Mapa de la Ciudad - EasyCab")
    city_map = CityMap(root, size)

    load_city_map(city_map)

    # Crear hilo para el manejo de comandos de usuario
    command_thread = threading.Thread(target=command_input_handler, daemon=True)
    command_thread.start()

    # Crear hilo para manejar las solicitudes de autenticación por sockets
    auth_thread = threading.Thread(target=handle_auth_requests_sockets, args=('localhost', 7999, city_map), daemon=True)
    auth_thread.start()

    # Crear hilo para manejar actualizaciones del taxi
    updates_thread = threading.Thread(target=handle_taxi_updates, args=(city_map,), daemon=True)
    updates_thread.start()

    # Crear hilo para manejar solicitudes de clientes
    customer_requests_thread = threading.Thread(target=handle_customer_requests, daemon=True)
    customer_requests_thread.start()

    root.mainloop()