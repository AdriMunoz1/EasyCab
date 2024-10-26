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


# Función para autenticar el taxi
def authenticate_taxi(id_taxi):
    try:
        connection = sqlite3.connect('taxis.db')
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM taxis WHERE id = ?', (id_taxi,))
        taxi = cursor.fetchone()
        connection.close()

        if taxi:
            print(f"Taxi {id_taxi} autenticado correctamente.")
            update_taxi_status("OK", int(id_taxi))
            return True

        else:
            print(f"Taxi {id_taxi} no está registrado.")
            return False

    except sqlite3.Error as e:
        print(f"Error al autenticar el taxi: {e}")
        return False


def move_taxi_to_destination(id_taxi, position_taxi, position_customer, destination):
    # Mover taxi a la posición del cliente
    while position_taxi != position_customer:
        # Lógica para mover el taxi un paso hacia el cliente
        position_taxi = (position_taxi[0] + 1 if position_taxi[0] < position_customer[0] else position_taxi[0],
                            position_taxi[1] + 1 if position_taxi[1] < position_customer[1] else position_taxi[1])
        time.sleep(1)  # Simular tiempo de movimiento
        print(f"Taxi {id_taxi} en {position_taxi}")

    # Una vez en la posición del cliente, mover al destino
    while position_taxi != destination:
        position_taxi = (position_taxi[0] + 1 if position_taxi[0] < destination[0] else position_taxi[0],
                            position_taxi[1] + 1 if position_taxi[1] < destination[1] else position_taxi[1])
        time.sleep(1)  # Simular tiempo de movimiento
        print(f"Taxi {id_taxi} en {position_taxi}")

    print(f"Taxi {id_taxi} ha llegado a su destino {destination}.")


# Función para manejar las conexiones con los taxis
def handle_taxis(sockets_taxi, socket_taxi, addr, city_map):
    id_taxi = None
    position_taxi = None

    try:
        while True:
            request = socket_taxi.recv(1024).decode("utf-8")
            if not request:
                break

            if request.startswith("AUTH#"):
                id_taxi = request.split("#")[1]
                print(f"Autenticando taxi {id_taxi}")

                if authenticate_taxi(id_taxi):
                    response = "OK"
                    sockets_taxi.append({"socket": socket_taxi, "taxi_id": id_taxi})

                    AVAILABLE_TAXIS.append(id_taxi)  # Agregar taxi a la lista de disponibles

                    position_taxi = (1, 1)
                    city_map[position_taxi[0]][position_taxi[1]] = f'T{id_taxi} verde'

                else:
                    response = "KO"

                socket_taxi.send(response.encode("utf-8"))

            elif request.startswith("STOP#"):
                if id_taxi:
                    city_map[position_taxi[0]][position_taxi[1]] = f'T{id_taxi} rojo'
                    update_taxi_status("KO", id_taxi)

                    AVAILABLE_TAXIS.remove(id_taxi)  # Remover taxi de la lista de disponibles

                    print(f"Taxi {id_taxi} detenido y marcado como KO")

                else:
                    response = "Taxi no autenticado"
                    socket_taxi.send(response.encode("utf-8"))

            elif request.startswith("RESUME#"):
                if id_taxi:
                    city_map[position_taxi[0]][position_taxi[1]] = f'T{id_taxi} verde'
                    update_taxi_status("OK", id_taxi)

                    AVAILABLE_TAXIS.append(id_taxi)  # Agregar taxi a la lista de disponibles

                    print(f"Taxi {id_taxi} reanudado y marcado como OK")

                else:
                    response = "Taxi no autenticado"
                    socket_taxi.send(response.encode("utf-8"))

            elif request.startswith("DESTINATION#"):
                if id_taxi:
                    # Extraer información del destino y la posición del cliente
                    parts = request.split("#")
                    id_site = parts[2]
                    position_customer = (int(parts[3]), int(parts[4]))  # La posición del cliente

                    for x in range(20):
                        for y in range(20):
                            if city_map[x][y] == id_site:
                                next_position_taxi = (x, y)

                                if position_taxi:
                                    city_map[position_taxi[0]][position_taxi[1]] = ''

                                position_taxi = next_position_taxi
                                city_map[next_position_taxi[0]][next_position_taxi[1]] = f'T{id_taxi} verde'
                                socket_taxi.send(f"DESTINATION#{id_taxi}#{next_position_taxi}".encode("utf-8"))
                                print(f"Taxi {id_taxi} cambiado a destino {next_position_taxi}")

                                # Iniciar el hilo para mover el taxi usando la posición del cliente
                                movement_thread = threading.Thread(target=move_taxi_to_destination,
                                                                   args=(id_taxi, position_taxi, position_customer, next_position_taxi))
                                movement_thread.start()  # Inicia el hilo para mover el taxi
                                break

                else:
                    response = "Taxi no autenticado"
                    socket_taxi.send(response.encode("utf-8"))

            elif request.startswith("RETURN#"):
                if id_taxi:
                    if position_taxi:
                        city_map[position_taxi[0]][position_taxi[1]] = ''
                    position_taxi = (1, 1)
                    city_map[1][1] = f'T{id_taxi} verde'
                    socket_taxi.send(f"RETURN#{id_taxi}".encode("utf-8"))
                    print(f"Taxi {id_taxi} regresando a la base")

                else:
                    response = "Taxi no autenticado"
                    socket_taxi.send(response.encode("utf-8"))

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if id_taxi:
            update_taxi_status("KO", id_taxi)
            sockets_taxi[:] = [cs for cs in sockets_taxi if cs["taxi_id"] != id_taxi]

            AVAILABLE_TAXIS.remove(id_taxi)  # Remover taxi de la lista de disponibles

            if position_taxi:
                city_map[position_taxi[0]][position_taxi[1]] = ''

            print(f"Taxi {id_taxi} desconectado y marcado como KO.")
            socket_taxi.close()
            print(f"Conexión con {addr[0]}:{addr[1]} cerrada.")


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
    ip_central = "localhost"     # Hay que poner la de la máquina
    #ip_central = get_ip()

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

        threading.Thread(target=handle_command_input, args=(sockets_taxi,)).start()

        while True:
            socket_taxi, addr = socket_central.accept()
            print(f"Conexión aceptada de {addr[0]}:{addr[1]}")
            thread = threading.Thread(target=handle_taxis, args=(sockets_taxi, socket_taxi, addr, city_map))
            thread.start()

    except Exception as e:
        print(f"Error en el servidor: {e}")
    finally:
        socket_central.close()


# Ejecutar el servidor EC_Central
if __name__ == "__main__":
    city_map = load_city_map('city_map.txt')
    run_server(city_map)