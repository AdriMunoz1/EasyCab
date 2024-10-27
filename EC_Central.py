import socket
import sqlite3
import sys
import threading
import time
from kafka import KafkaProducer, KafkaConsumer


AVAILABLE_TAXIS = []    # Lista para almacenar los taxis disponibles
SIZE = 20
CELL_SIZE = 30

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


def wait_taxi_in_destination(socket_taxi, id_taxi):
    # Esperar la confirmación de llegada a la posición
    while True:
        confirmation = socket_taxi.recv(1024).decode('utf-8')
        if confirmation.startswith("DESTINO:"):
            reached_destination = confirmation.split(":")[1].split("#")[0]
            print(f"TAXI#{id_taxi} ha alcanzado la POSICION#{reached_destination}.")
            break


def handle_destinations(consumer, producer, topic_response):
    global AVAILABLE_TAXIS

    print("EC_Central está escuchando destinos desde Customer...")

    for message in consumer:
        destination_data = message.value.decode('utf-8')
        print(f"Destino recibido de EC_Customer: {destination_data}")

        # Procesar el destino recibido
        try:
            id_client, destination, position_customer = destination_data.split("#")

            # Verificar si hay taxis disponibles
            if AVAILABLE_TAXIS:
                # Obtener el primer taxi disponible (eliminarlo temporalmente de la lista)
                id_taxi, socket_taxi = AVAILABLE_TAXIS.popitem()

                try:
                    # Notificar a EC_Customer que la solicitud ha sido aceptada
                    message_response = "OK"
                    producer.send(topic_response, message_response.encode('utf-8'))
                    print(f"Notificando a EC_Customer: Servicio ACEPTADO para cliente {id_client}")

                    # Asignar la posición del customer al taxi
                    socket_taxi.sendall(f"DESTINATION#{position_customer}".encode('utf-8'))
                    print(f"Destino {position_customer} asignado al taxi {id_taxi}")

                    # Esperar que el taxi llegue a la posición del customer
                    wait_taxi_in_destination(socket_taxi, id_taxi)

                    # Luego enviar al taxi a la posición del destino
                    socket_taxi.sendall(f"DESTINATION#{destination}".encode('utf-8'))
                    print(f"Destino {destination} asignado al taxi {id_taxi}")

                    # Esperar que el taxi llegue a la posición del destino
                    wait_taxi_in_destination(socket_taxi, id_taxi)

                    # Finalmente, enviar al taxi de regreso a la posición (1, 1)
                    socket_taxi.sendall("RETURN".encode('utf-8'))
                    print(f"Destino (1, 1) asignado al taxi {id_taxi}")

                    # Esperar que el taxi llegue a la posición del destino
                    wait_taxi_in_destination(socket_taxi, id_taxi)

                except (BrokenPipeError, ConnectionResetError):
                    print(f"No se pudo enviar el destino al taxi {id_taxi}. Conexión perdida.")
                    message_response = "DENIED"
                    producer.send(topic_response, message_response.encode('utf-8'))
                    print(f"Notificando a EC_Customer: Servicio DENEGADO para cliente {id_client}")

            else:
                # No hay taxis disponibles, notificar denegación
                message_response = "DENIED"
                producer.send(topic_response, message_response.encode('utf-8'))
                print(f"Notificando a EC_Customer: Servicio DENEGADO para cliente {id_client}")

        except ValueError:
            print(f"Error en el formato de mensaje recibido: {destination_data}")


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
            return True
        else:
            print(f"Taxi {id_taxi} no está registrado.")
            return False
    except sqlite3.Error as e:
        print(f"Error al autenticar el taxi: {e}")
        return False


def handle_taxi(socket_taxi, address_taxi, city_map):
    global AVAILABLE_TAXIS

    try:
        while True:
            message = socket_taxi.recv(1024).decode('utf-8')

            if not message:  # Si se recibe un mensaje vacío, el taxi cerró la conexión
                print(f"Conexión cerrada por el taxi {address_taxi}.")
                socket_taxi.close()
                break

            if message.startswith("AUTH#"):
                id_taxi = message.split("#")[1]

                if authenticate_taxi(id_taxi):
                    response = "OK"
                    socket_taxi.send(response.encode("utf-8"))
                    AVAILABLE_TAXIS.append({"socket": socket_taxi, "taxi_id": id_taxi})
                    taxi_position = (1, 1)
                    city_map[taxi_position[0]][taxi_position[1]] = f'T{id_taxi} verde'

                else:
                    response = "KO"
                    socket_taxi.send(response.encode("utf-8"))
                    print(f"Taxi {id_taxi} no autenticado. Cerrando conexión.")
                    socket_taxi.close()  # Cerrar la conexión con el taxi no autenticado
                    return  # Salir del bucle y finalizar el hilo para este taxi

            elif message.startswith("DESTINO:"):
                # Procesar el mensaje de destino alcanzado
                reached_destination = message.split(":")[1]
                print(f"TAXI ha alcanzado el destino: {reached_destination}")

            elif message.startswith("SENSOR_OK"):
                print(f"TAXI en correcto funcionamiento")

            elif message.startswith("SENSOR_KO"):
                print(f"TAXI detenido por una incidencia")

            else:
                print("Mensaje no reconocido de", address_taxi, ":", message)

    except (ConnectionResetError, BrokenPipeError):
        print(f"Conexión perdida con el taxi {address_taxi}.")
        # Eliminar taxi de la lista de disponibles
        for id_taxi, sock in list(AVAILABLE_TAXIS.items()):
            if sock == socket_taxi:
                del AVAILABLE_TAXIS[id_taxi]
                print(f"Taxi {id_taxi} eliminado de la lista de disponibles.")
                break

    except (ConnectionResetError, BrokenPipeError):
        print(f"Conexión perdida con el taxi {address_taxi}.")
        # Eliminar taxi de la lista de disponibles
        for id_taxi, sock in list(AVAILABLE_TAXIS.items()):
            if sock == socket_taxi:
                del AVAILABLE_TAXIS[id_taxi]
                print(f"Taxi {id_taxi} eliminado de la lista de disponibles.")
                break


def handle_positions(consumer):
    print("EC_Central está escuchando posiciones de los taxis...")

    for message in consumer:
        try:
            position_taxi = message.value.decode('utf-8')
            print(f"Posición recibida de Kafka: {position_taxi}")

            # Aquí podrías agregar lógica para almacenar o procesar la posición
            # Ejemplo: Actualizar la posición de un taxi en un diccionario
            # id_taxi, coord_x, coord_y = position_taxi.split("#")
            # AVAILABLE_TAXIS[id_taxi]['position'] = (coord_x, coord_y)

        except Exception as e:
            print(f"Error al procesar el mensaje de posición: {e}")


def send_command_to_taxi(taxi_id, command):
    if taxi_id in AVAILABLE_TAXIS:
        socket_taxi = AVAILABLE_TAXIS[taxi_id]
        socket_taxi.sendall(command.encode('utf-8'))
        print(f"Comando enviado a TAXI#{taxi_id}: {command}")
    else:
        print(f"TAXI#{taxi_id} no está disponible.")


def command_listener():
    while True:
        id_taxi = input("Ingrese ID del taxi: ").strip()
        command = input("Ingrese comando (STOP, RESUME, DESTINATION#<destino>, RETURN): ").strip()

        if command in ["STOP", "RESUME", "RETURN"]:
            send_command_to_taxi(id_taxi, command)

        elif command.startswith("DESTINATION#"):
            destination = command.split("#", 1)[1].strip()
            send_command_to_taxi(id_taxi, f"DESTINATION#{destination}")

        else:
            print("Comando no reconocido.")


def main():
    port_central, ip_broker, port_broker = get_parameters()
    ip_central = get_ip()
    city_map = load_city_map('city_map.txt')

    # Configurar Kafka Producer para enviar respuestas a los clientes
    topic_central_customer = 'respuesta_central'
    producer = KafkaProducer(bootstrap_servers=f'{ip_broker}:{port_broker}')

    # Configurar Kafka Consumer para recibir solicitudes de destinos de los clientes
    topic_customer_central = "solicitud_central"
    consumer_customer = KafkaConsumer(
        topic_customer_central,
        bootstrap_servers=f'{ip_broker}:{port_broker}',
        group_id='requests_customer',
        auto_offset_reset='earliest'
    )

    # Iniciar hilo para manejar las solicitudes de destinos de EC_Customer
    threading.Thread(target=handle_destinations, args=(consumer_customer, producer, topic_central_customer), daemon=True).start()

    # Configurar Kafka Consumer para recibir posiciones de los taxis
    topic_taxi_central = "positions_topic"  # Cambia esto al nombre del topic que usarás
    consumer_taxi = KafkaConsumer(
        topic_taxi_central,
        bootstrap_servers=f'{ip_broker}:{port_broker}',
        group_id='positions_taxi',
        auto_offset_reset='earliest'
    )

    # Iniciar hilo para manejar las posiciones de los taxis
    threading.Thread(target=handle_positions, args=(consumer_taxi,), daemon=True).start()

    # Configurar servidor de socket para recibir conexiones de taxis
    socket_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socket_server.bind((ip_central, port_central))
    socket_server.listen(5)
    print(f"EC_Central está en escucha en {ip_central}:{port_central}")

    # Iniciar el hilo para escuchar comandos desde la terminal
    threading.Thread(target=command_listener, daemon=True).start()

    try:
        while True:
            socket_taxi, address_taxi = socket_server.accept()
            print(f"Conexión aceptada de {address_taxi}")

            # Crear un nuevo hilo para manejar la conexión de cada taxi
            threading.Thread(target=handle_taxi, args=(socket_taxi, address_taxi, city_map)).start()

    except Exception as e:
        print(f"Error al conectar con EC_Central: {e}")

    finally:
        socket_server.close()


if __name__ == "__main__":
    main()