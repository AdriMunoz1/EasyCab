import socket
import sqlite3
import threading
import json
import time
from confluent_kafka import Producer, Consumer
from tkinter import Tk, Canvas
import sys
import signal

# Configuración de Kafka
KAFKA_SERVER = 'localhost:9092'

#producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

producer_conf = {'bootstrap.servers': KAFKA_SERVER}
producer = Producer(producer_conf)

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

    def get_location_coords(self, label):
        if label in self.locations:
            rect_id, _ = self.locations[label]
            x0, y0, x1, y1 = self.canvas.coords(rect_id)
            x = x1 // self.cell_size
            y = y1 // self.cell_size
            return f"{int(x)},{int(y)}"
        return "0,0"
    
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
    producer.produce(
        topic='taxi_commands',
        value=json.dumps(msg).encode('utf-8')
    )
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
    consumer_conf = {
        'bootstrap.servers': KAFKA_SERVER,
        'group.id': 'central_taxi_updates_group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe(['taxi_updates'])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[Central] Error consumidor taxi_updates: {msg.error()}")
                continue

            update = json.loads(msg.value().decode('utf-8'))
            taxi_id = update.get('taxi_id')
            status = update.get('status')
            position = update.get('position')

            if status == 'moving':
                city_map.move_taxi(taxi_id, position[0], position[1], 'green')
            elif status == 'stopped':
                city_map.move_taxi(taxi_id, position[0], position[1], 'red')

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
    

def handle_customer_requests():
    consumer_conf = {
        'bootstrap.servers': KAFKA_SERVER,
        'group.id': 'central_service_requests_group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe(['service_requests'])

    print("[Central] Esperando solicitudes de clientes...")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[Central] Error consumidor solicitudes: {msg.error()}")
            continue

        request = json.loads(msg.value().decode('utf-8'))
        request_id = request.get("RequestId")
        client_id = request.get("ClientId")
        position = request.get("Position")
        destination = request.get("Destination")

        print(f"[Central] Recibida solicitud de servicio: RequestId={request_id}, Cliente={client_id}, Posición={position}, Destino={destination}")

        time.sleep(1)

        # ================================
        # Decidir si aceptar o denegar:
        # Por ahora, aceptar siempre.
        accept_request = True

        if accept_request:
            # Responder 'accepted'
            response_accepted = {
                "RequestId": request_id,
                "ClientId": client_id,
                "Status": "accepted"
            }
            producer.produce(
                topic='service_responses',
                value=json.dumps(response_accepted).encode('utf-8')
            )            
            producer.flush()
            print(f"[Central] Respondido ACCEPTED para RequestId={request_id}")

            dest_coord = city_map.get_location_coords(destination)
            print(f"[DEBUG] get_location_coords({destination}) => {dest_coord} ({type(dest_coord)})")

            # Simular trayecto terminado INSTANTÁNEAMENTE
            response_completed = {
                "RequestId": request_id,
                "ClientId": client_id,
                "Status": "completed",
                "NewPosition": str(dest_coord)
            }
            producer.produce(
                topic='service_responses',
                value=json.dumps(response_completed).encode('utf-8')
            )
            producer.flush()
            print(f"[Central] Respondido COMPLETED para RequestId={request_id} con NewPosition={dest_coord}")

        else:
            # Responder 'denied'
            response_denied = {
                "RequestId": request_id,
                "ClientId": client_id,
                "Status": "denied"
            }
            producer.send('service_responses', response_denied)
            producer.flush()
            print(f"[Central] Respondido DENIED para RequestId={request_id}")


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

    # Crear hilo para manejar actualizaciones del taxi
    customers_thread = threading.Thread(target=handle_customer_requests, daemon=True)
    customers_thread.start()

    root.mainloop()
