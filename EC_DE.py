import json
import socket
import threading
import sys
import time
from kafka import KafkaProducer, KafkaConsumer

# Variables globales
STATUS_TAXI = "OK"                      # Estado del taxi -> OK, KO
POSITION_TAXI = (1, 1)                  # Posición inicial
DESTINATION_TAXI = None                 # Destino del taxi
SOCKET_SENSOR = None                    # Socket del sensor
THREAD_TAXI = None                      # Hilo de movimiento
IS_MOVING_THREAD_RUNNING = False        # Variable de control del hilo de movimiento
LOCK_STATUS_TAXI = threading.Lock()     # Lock para el estado del taxi


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


# Validar el taxi
def validate_taxi(socket_central, id_taxi):
    try:
        msg = f"AUTH#{id_taxi}"
        socket_central.send(msg.encode("utf-8"))
        response = socket_central.recv(1024).decode("utf-8")
        print(f"Respuesta de EC_Central: {response}")

        if response == "OK":
            print("Taxi SÍ autenticado")
            return True
        else:
            print("Taxi NO autenticado")
            return False

    except Exception as e:
        print(f"Error al autenticar el taxi: {e}")
        return False


# Enviar posición a EC_Central
def send_taxi_position_to_central(socket_central, id_taxi):
    try:
        message = f"POSITION#{id_taxi}#{POSITION_TAXI}"
        socket_central.send(message.encode('utf-8'))

    except Exception as e:
        print(f"Error al enviar la posición del taxi {id_taxi}: {e}")


# Mover taxi
def move_taxi_to_destination(destination, socket_central, id_taxi):
    global STATUS_TAXI, POSITION_TAXI, IS_MOVING_THREAD_RUNNING

    if POSITION_TAXI is None or destination is None:
        print("Error: Posiciones no inicializadas correctamente.")
        IS_MOVING_THREAD_RUNNING = False
        return

    while POSITION_TAXI != destination:
        with LOCK_STATUS_TAXI:
            if STATUS_TAXI == "KO":
                print(f"Taxi {id_taxi} detenido en {POSITION_TAXI}")
                IS_MOVING_THREAD_RUNNING = False
                return

        # Actualizar posición en eje X
        if POSITION_TAXI[0] < destination[0]:
            POSITION_TAXI = (POSITION_TAXI[0] + 1, POSITION_TAXI[1])
        elif POSITION_TAXI[0] > destination[0]:
            POSITION_TAXI = (POSITION_TAXI[0] - 1, POSITION_TAXI[1])

        # Actualizar posición en eje Y
        if POSITION_TAXI[1] < destination[1]:
            POSITION_TAXI = (POSITION_TAXI[0], POSITION_TAXI[1] + 1)
        elif POSITION_TAXI[1] > destination[1]:
            POSITION_TAXI = (POSITION_TAXI[0], POSITION_TAXI[1] - 1)

        time.sleep(1)
        send_taxi_position_to_central(socket_central, id_taxi)
        print(f"Taxi {id_taxi} moviéndose a {POSITION_TAXI}")

    if POSITION_TAXI == destination:
        print(f"Taxi {id_taxi} ha llegado a su destino: {destination}")

        if SOCKET_SENSOR:
            SOCKET_SENSOR.send(b'STOP')

    IS_MOVING_THREAD_RUNNING = False


# Iniciar el hilo de movimiento
def start_moving_thread(socket_central, id_taxi):
    global THREAD_TAXI, IS_MOVING_THREAD_RUNNING

    if not IS_MOVING_THREAD_RUNNING:
        THREAD_TAXI = threading.Thread(target=move_taxi_to_destination, args=(DESTINATION_TAXI, socket_central, id_taxi))
        THREAD_TAXI.start()
        IS_MOVING_THREAD_RUNNING = True


# Manejar señales de EC_S
def handle_signals_sensor(socket_sensor, addr, socket_central, id_taxi):
    global SOCKET_SENSOR, STATUS_TAXI

    SOCKET_SENSOR = socket_sensor
    try:
        while True:
            signal = socket_sensor.recv(1024).decode('utf-8')
            if signal == "KO":
                print("Sensor envió KO, notificando a EC_Central para detener el taxi")
                msg = f"STOP#{id_taxi}"
                socket_central.send(msg.encode("utf-8"))

                with LOCK_STATUS_TAXI:
                    STATUS_TAXI = "KO"

                time.sleep(5)
                msg = f"RESUME#{id_taxi}"
                socket_central.send(msg.encode("utf-8"))

                with LOCK_STATUS_TAXI:
                    STATUS_TAXI = "OK"

                start_moving_thread(socket_central, id_taxi)

    except Exception as e:
        print(f"Error manejando la señal del sensor {addr}: {e}")

    finally:
        socket_sensor.close()


# Escuchar señales de EC_S
def run_server_sensor(socket_central, ip_s, port_s, id_taxi):
    socket_taxi = None

    try:
        socket_taxi = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_taxi.bind((ip_s, port_s))
        socket_taxi.listen()
        print(f"Servidor de sensores del taxi {id_taxi} escuchando en {ip_s}:{port_s}")

        while True:
            socket_sensor, addr = socket_taxi.accept()
            print(f"Conexión aceptada de {addr[0]}:{addr[1]}")
            thread_taxi = threading.Thread(target=handle_signals_sensor, args=(socket_sensor, addr, socket_central, id_taxi))
            thread_taxi.start()

    except Exception as e:
        print(f"Error en el servidor del taxi: {e}")

    finally:
        socket_taxi.close()


# Manejador de comandos de EC_Central
def handle_commands_central(central_socket, id_taxi):
    global STATUS_TAXI, DESTINATION_TAXI, SOCKET_SENSOR

    try:
        while True:
            message = central_socket.recv(1024).decode("utf-8")

            if message.startswith("STOP#"):
                with LOCK_STATUS_TAXI:
                    print("Recibido comando para detener el taxi")
                    STATUS_TAXI = "KO"

                central_socket.send("OK".encode('UTF-8'))

                if SOCKET_SENSOR:
                    SOCKET_SENSOR.send(b'STOP')

            elif message.startswith("RESUME#"):
                with LOCK_STATUS_TAXI:
                    print("Recibido comando para reanudar el taxi")
                    STATUS_TAXI = "OK"

                central_socket.send("OK".encode('UTF-8'))

                if SOCKET_SENSOR:
                    SOCKET_SENSOR.send(b'START')

                start_moving_thread(central_socket, id_taxi)

            elif message.startswith("DESTINATION#"):
                parts = message.split("#")
                DESTINATION_TAXI = eval(parts[2])
                print(f"Recibido nuevo destino: {DESTINATION_TAXI}")

                if SOCKET_SENSOR:
                    SOCKET_SENSOR.send(b'START')

                with LOCK_STATUS_TAXI:
                    STATUS_TAXI = "OK"

                start_moving_thread(central_socket, id_taxi)

            elif message.startswith("RETURN#"):
                print("Recibido comando para volver a la base [1,1]")
                DESTINATION_TAXI = (1, 1)

                if SOCKET_SENSOR:
                    SOCKET_SENSOR.send(b'START')

                with LOCK_STATUS_TAXI:
                    STATUS_TAXI = "OK"

                start_moving_thread(central_socket, id_taxi)

    except Exception as e:
        print(f"Error al recibir comandos: {e}")


def handle_kafka_messages(consumer, producer, topic_central, id_taxi):
    global DESTINATION_TAXI, STATUS_TAXI

    try:
        for message in consumer:
            command = message.value

            if "destination" in command:
                DESTINATION_TAXI = tuple(command["destination"])
                print(f"Recibido nuevo destino por Central: {DESTINATION_TAXI}")

                with LOCK_STATUS_TAXI:
                    STATUS_TAXI = "OK"

                start_moving_thread(producer, id_taxi)

    except Exception as e:
        print(f"Error al manejar mensajes de Kafka: {e}")


# Función principal
def main():
    ip_central, port_central, ip_broker, port_broker, ip_s, port_s, id_taxi = get_parameters()

    socket_central = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socket_central.connect((ip_central, port_central))

    if validate_taxi(socket_central, id_taxi):

        # Crear productor de Kafka
        producer = KafkaProducer(
            bootstrap_servers=f"{ip_broker}:{port_broker}",
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # Crear consumidor de Kafka
        topic_consumer = f"TAXI#{id_taxi}"
        consumer = KafkaConsumer(
            topic_consumer,
            bootstrap_servers=f"{ip_broker}:{port_broker}",
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        thread_taxi = threading.Thread(target=run_server_sensor, args=(socket_central, ip_s, port_s, id_taxi))
        thread_taxi.start()

        thread_command_central = threading.Thread(target=handle_commands_central, args=(socket_central, id_taxi))
        thread_command_central.start()

        # Iniciar thread para manejar mensajes de Kafka
        thread_central = threading.Thread(target=handle_kafka_messages, args=(consumer, producer, topic_consumer, id_taxi))
        thread_central.start()

    else:
        print("Autenticación fallida.")
        socket_central.close()


if __name__ == "__main__":
    main()