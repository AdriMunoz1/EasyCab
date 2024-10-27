import socket
import sqlite3
import sys
import threading
import time
from kafka import KafkaProducer, KafkaConsumer


AVAILABLE_TAXIS = []    # Lista para almacenar los taxis disponibles


def get_parameters():
    if len(sys.argv) != 4:
        print(f"Error: python3 EC_Central.py <Port Central> <IP Broker> <Port Broker>")
        sys.exit(1)

    port_central = int(sys.argv[1])
    ip_broker = sys.argv[2]
    port_broker = int(sys.argv[3])

    return port_central, ip_broker, port_broker


def get_ip():
    # Crear un socket para obtener la IP local
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        # No necesitas conectarte a un servidor, solo necesitas una dirección válida
        s.connect(("8.8.8.8", 80))  # Conéctate a un servidor público (Google DNS)
        ip_address = s.getsockname()[0]  # Obtén la dirección IP de la máquina

    finally:
        s.close()  # Cierra el socket

    return ip_address


def load_city_map(filename):
    city_map = [['' for _ in range(20)] for _ in range(20)]

    try:
        with open(filename, 'r') as file:
            for line in file:
                parts = line.strip().split()

                if len(parts) == 3:
                    id_site = parts[0]
                    coord_x = int(parts[1])
                    coord_y = int(parts[2])
                    city_map[coord_x][coord_y] = id_site

        print("Mapa de la ciudad cargado correctamente.")

    except Exception as e:
        print(f"Error al cargar el mapa de la ciudad: {e}")

    return city_map


# Función para actualizar el estado de un taxi en la base de datos
def update_taxi_status(status_taxi, id_taxi):
    try:
        connection = sqlite3.connect('taxis.db')
        cursor = connection.cursor()
        cursor.execute('UPDATE taxis SET estado = ? WHERE id = ?', (status_taxi, id_taxi))
        connection.commit()
        connection.close()
        print(f"Taxi {id_taxi} actualizado a {status_taxi}.")

    except sqlite3.Error as e:
        print(f"Error al actualizar el estado del taxi {id_taxi}: {e}")


# Función para enviar un comando al taxi (STOP, RESUME, DESTINATION, RETURN)
def send_command(sockets_taxi, command, id_taxi, destination = None):
    try:
        for socket_taxi in sockets_taxi:
            if socket_taxi["taxi_id"] == id_taxi:
                if command == "DESTINATION":
                    msg = f"{command}#{id_taxi}#{destination}"

                else:
                    msg = f"{command}#{id_taxi}"

                socket_taxi["socket"].send(msg.encode('utf-8'))
                print(f"Comando {command} enviado al taxi {id_taxi}")

                # Recibir respuesta del taxi solo para los comandos RESUME y DESTINATION
                if command in ["RESUME", "DESTINATION"]:
                    message = socket_taxi["socket"].recv(1024).decode('utf-8')

                    if message == 'OK' and command == 'STOP':
                        update_taxi_status("KO", id_taxi)

                    elif message == 'OK' and command == 'RESUME':
                        update_taxi_status("OK", id_taxi)

                return

        print(f"Taxi {id_taxi} no encontrado.")

    except Exception as e:
        print(f"Error al enviar el comando {command} al taxi {id_taxi}: {e}")


# Función para autenticar el taxi a través de sockets
def authenticate_taxi(socket_taxi):
    try:
        id_taxi = socket_taxi.recv(1024).decode('utf-8')
        print(f"Solicitud de autenticación recibida del taxi {id_taxi}.")
        connection = sqlite3.connect('taxis.db')
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM taxis WHERE id = ?', (id_taxi,))
        taxi = cursor.fetchone()
        connection.close()

        if taxi:
            print(f"Taxi {id_taxi} autenticado correctamente.")
            update_taxi_status("OK", int(id_taxi))
            return id_taxi  # Devuelve el ID del taxi autenticado
        else:
            print(f"Taxi {id_taxi} no está registrado.")
            socket_taxi.send(b"DENIED")
            return None

    except sqlite3.Error as e:
        print(f"Error al autenticar el taxi: {e}")
        return None


# Hilo para manejar los comandos del usuario
def handle_command_input(sockets_taxi):
    while True:
        if not AVAILABLE_TAXIS:
            print("No hay taxis registrados. Esperando a que un taxi se registre...")
            time.sleep(5)  # Esperar un momento antes de volver a comprobar
            continue  # Volver al inicio del bucle y comprobar nuevamente

        id_taxi = input("Introduce el ID del taxi: ")
        command = input("Introduce el número del comando (1.STOP/2.RESUME/3.DESTINATION/4.RETURN) para el taxi: ")

        if command in ["1", "2"]:
            if command == "1":
                command = "STOP"

            else:
                command = "RESUME"

            send_command(sockets_taxi, command, id_taxi)

        elif command == "3":
            destination = input("Introduce la nueva localización (ID_LOCALIZACION): ")
            send_command(sockets_taxi, "DESTINATION", id_taxi, destination)

        elif command == "4":
            send_command(sockets_taxi, "RETURN", id_taxi)

        else:
            print("Comando no válido. Usa 1, 2, 3 o 4.")


# Función para manejar la comunicación con los taxis utilizando Kafka
def handle_taxis(consumer, producer, topic_producer):
    for message in consumer:
        request = message.value.decode('utf-8')
        id_taxi, command = request.split('#')

        print(f"Solicitud recibida del taxi {id_taxi}: {command}")

        # Procesar la solicitud del taxi
        if command.startswith("ARRIVED"):
            destination = command.split(' ')[1]
            # Procesar llegada a destino
            print(f"El taxi {id_taxi} ha llegado a su destino {destination}.")

        elif command.startswith("POSITION"):
            position_data = command.split(' ')[1:]  # Asumiendo que las posiciones están después de la palabra POSITION
            print(f"Posición del taxi {id_taxi}: {', '.join(position_data)}")  # Imprimir la posición recibida

        elif command == "STATUS":
            # Enviar estado del taxi a EC_Central
            response = f"STATUS_UPDATE#{id_taxi}#OK"  # Ejemplo de respuesta
            producer.send(topic_producer, response.encode('utf-8'))
            print(f"Estado del taxi {id_taxi} enviado a EC_Central.")

        # Simular tiempo de procesamiento
        time.sleep(1)


# Función para manejar solicitudes de customer con Kafka
def handle_customers(sockets_taxi, consumer, producer, topic_producer):
    for message in consumer:
        request = message.value.decode('utf-8')
        id_customer, destination = request.split('#')

        destination = destination.strip("() ")

        print(f"Solicitud recibida de {id_customer} para destino {destination}")

        time.sleep(2)  # Simular tiempo de procesamiento

        # Aquí se decide si se acepta o se rechaza la solicitud
        if AVAILABLE_TAXIS:
            # Si hay taxis disponibles, aceptar la solicitud
            id_taxi = AVAILABLE_TAXIS.pop(0)  # Asigna el primer taxi disponible

            # Enviar destino al taxi
            send_command(sockets_taxi, "DESTINATION", id_taxi, destination)

            response = f"OK#{id_taxi}"  # Respuesta con el ID del taxi
        else:
            # Si no hay taxis disponibles, denegar la solicitud
            response = "DENEGADO"

        producer.send(topic_producer, response.encode('utf-8'))
        print(f"Respuesta enviada a {id_customer}: {response}")


# Función para ejecutar el servidor EC_Central
def run_server(city_map):
    port_central, ip_broker, port_broker = get_parameters()
    ip_central = "localhost"  # Hay que poner la de la máquina
    # ip_central = get_ip()

    sockets_taxi = []

    socket_central = None

    try:
        socket_central = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_central.bind((ip_central, port_central))
        socket_central.listen()
        print(f"EC_Central escuchando en {ip_central}:{port_central}")

        # Configurar consumidor de Kafka para recibir solicitudes
        topic_consumer = 'solicitud_central'
        consumer = KafkaConsumer(
            topic_consumer,
            bootstrap_servers=f'{ip_broker}:{port_broker}',
            group_id='central',
            auto_offset_reset='earliest'
        )

        # Configurar productor de Kafka para enviar respuesta
        topic_producer = 'respuesta_central'
        producer = KafkaProducer(bootstrap_servers=f'{ip_broker}:{port_broker}')

        # Crear un hilo para manejar las solicitudes de taxis con Kafka
        kafka_thread = threading.Thread(target=handle_customers, args=(sockets_taxi, consumer, producer, topic_producer))
        kafka_thread.start()

        # Crear un hilo para manejar la entrada de comandos del usuario
        command_thread = threading.Thread(target=handle_command_input, args=(sockets_taxi,), daemon=True)
        command_thread.start()

        while True:
            socket_taxi, addr = socket_central.accept()
            print(f"Conexión aceptada de {addr[0]}:{addr[1]}")
            sockets_taxi.append({"taxi_id": addr[1], "socket": socket_taxi})  # Almacenar el socket del taxi
            thread = threading.Thread(target=handle_taxis, args=(consumer, producer, topic_producer, socket_taxi, addr, city_map))
            thread.start()

    except Exception as e:
        print(f"Error en el servidor: {e}")
    finally:
        socket_central.close()


# Ejecutar el servidor EC_Central
if __name__ == "__main__":
    city_map = load_city_map('city_map.txt')
    run_server(city_map)