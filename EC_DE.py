import socket
import sys
import threading
import time

from kafka import KafkaProducer

SIZE_MAP = 20               # Tamaño del mapa (20x20)
POSITION_TAXI = (1, 1)      # Posición del taxi
DESTINATION = None          # Destino asignado al taxi
MOVING = False              # Controlador de si el taxi se encuentra en movimiento o no
STATUS_SENSOR = "KO"        # Estado del sensor (OK / KO)


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
        return True

    except Exception as e:
        print(f"Error al autenticar el taxi: {e}")
        return False


def notify_arrival(socket_central):
    global DESTINATION

    try:
        # Enviar notificación al central
        msg = f"DESTINO:{DESTINATION}#ALCANZADO"
        socket_central.send(msg.encode("utf-8"))
        print(f"Notificación enviada a EC_Central: {msg}")

    except Exception as e:
        print(f"Error al enviar notificación: {e}")


def send_position_to_central(producer, id_taxi):
    topic_central = "position_taxi"
    message = f"{id_taxi}#{POSITION_TAXI}"

    producer.send(topic_central, value=message.encode("utf-8"))
    producer.flush()
    print(f"Posición enviada a EC_Central: {POSITION_TAXI}")


def notify_sensor_stop(socket_central):
    global POSITION_TAXI

    try:
        # Enviar notificación a EC_Central sobre la detención por KO
        msg = f"SENSOR_KO#{POSITION_TAXI}"
        socket_central.send(msg.encode("utf-8"))
        print(f"Notificación enviada a EC_Central sobre detención: {msg}")

    except Exception as e:
        print(f"Error al enviar notificación de detención: {e}")


def notify_sensor_ok(socket_central):
    global POSITION_TAXI

    try:
        # Enviar notificación a EC_Central sobre la reactivación tras KO
        msg = f"SENSOR_OK#{POSITION_TAXI}"
        socket_central.send(msg.encode("utf-8"))
        print(f"Notificación enviada a EC_Central sobre reactivación: {msg}")

    except Exception as e:
        print(f"Error al enviar notificación de reactivación: {e}")


def move_taxi(socket_central, producer, id_taxi):
    global POSITION_TAXI, DESTINATION, MOVING, STATUS_SENSOR

    while POSITION_TAXI != DESTINATION:
        if not MOVING:
            print("Taxi detenido, no se moverá.")
            return

        if STATUS_SENSOR == "KO":
            print("Sensor en estado KO. Movimiento detenido.")
            MOVING = False
            notify_sensor_stop(socket_central)
            return  # Detener el movimiento

        print(f"Moviendo de {POSITION_TAXI} hacia {DESTINATION}")

        # Calcular dirección
        if POSITION_TAXI[0] < DESTINATION[0]:  # Mover hacia abajo
            POSITION_TAXI = (POSITION_TAXI[0] + 1, POSITION_TAXI[1])

        elif POSITION_TAXI[0] > DESTINATION[0]:  # Mover hacia arriba
            POSITION_TAXI = (POSITION_TAXI[0] - 1, POSITION_TAXI[1])

        elif POSITION_TAXI[1] < DESTINATION[1]:  # Mover hacia la derecha
            POSITION_TAXI = (POSITION_TAXI[0], POSITION_TAXI[1] + 1)

        elif POSITION_TAXI[1] > DESTINATION[1]:  # Mover hacia la izquierda
            POSITION_TAXI = (POSITION_TAXI[0], POSITION_TAXI[1] - 1)

        print(f"Taxi en nueva posición: {POSITION_TAXI}")

        # Enviar la nueva posición a Kafka
        send_position_to_central(producer, id_taxi)

        time.sleep(1)  # Esperar un segundo entre movimientos

    # Notificar a EC_Central al llegar al destino
    notify_arrival(socket_central)

    DESTINATION = None
    MOVING = False


def receive_commands(socket_central, producer, id_taxi):
    global DESTINATION, MOVING

    try:
        while True:
            message = socket_central.recv(1024).decode("utf-8")

            if message.startswith("DESTINATION#"):
                requested_destination = eval(message.split("#")[1])  # Convertir el string a tupla
                if not DESTINATION and not MOVING:
                    DESTINATION = requested_destination
                    MOVING = True
                    print(f"Nuevo destino asignado: {DESTINATION}")
                    move_taxi(socket_central, producer, id_taxi)

            elif message == "STOP":
                if MOVING:
                    print("Movimiento detenido.")
                    MOVING = False

            elif message == "RESUME":
                if DESTINATION and not MOVING:
                    print(f"Reanudando movimiento hacia {DESTINATION}.")
                    MOVING = True
                    move_taxi(socket_central, producer, id_taxi)

            elif message.startswith("RETURN"):
                if not DESTINATION and not MOVING:
                    DESTINATION = (1, 1)  # Regresar a la posición inicial
                    MOVING = True
                    print(f"Regresando a la posición inicial: {DESTINATION}")
                    move_taxi(socket_central, producer, id_taxi)

    except Exception as e:
        print(f"Error al recibir comandos: {e}")


def handle_sensor(socket_central, socket_sensor):
    global MOVING, STATUS_SENSOR

    try:
        while True:
            message = socket_sensor.recv(1024).decode("utf-8")
            if message == "OK":
                if STATUS_SENSOR == "KO":
                    notify_sensor_ok(socket_central)
                STATUS_SENSOR = "OK"
                print("Estado del sensor: OK")

            elif message == "KO":
                STATUS_SENSOR = "KO"
                print("Estado del sensor: KO. Deteniendo movimiento.")
                MOVING = False  # Detener el movimiento si recibe KO
                notify_sensor_stop(socket_central)  # Notificar a EC_Central que se detuvo por KO

    except Exception as e:
        print(f"Error al recibir mensajes del sensor: {e}")


# Función principal
def main():
    ip_central, port_central, ip_broker, port_broker, ip_s, port_s, id_taxi = get_parameters()

    # Crear socket para conexión con la central
    socket_central = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Crear socket para el sensor
    socket_sensor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        # Escuchar conexiones del sensor
        socket_sensor.bind((ip_s, port_s))
        socket_sensor.listen(1)
        print("Esperando conexión del sensor...")
        connection_sensor, _ = socket_sensor.accept()
        print("Sensor conectado.")

        # Conectar a la central
        socket_central.connect((ip_central, port_central))
        print(f"Conectado a EC_Central en {ip_central}:{port_central}")

        # Autenticarse
        if validate_taxi(socket_central, id_taxi):

            # Para enviar la posición del taxi a central
            producer = KafkaProducer(bootstrap_servers=f'{ip_broker}:{port_broker}')



            # Iniciar hilo para recibir destinos
            threading.Thread(target=receive_commands, args=(socket_central, producer, id_taxi), daemon=True).start()

            # Iniciar hilo para escuchar al sensor
            threading.Thread(target=handle_sensor, args=(socket_central, connection_sensor), daemon=True).start()

            # Mantener el programa en ejecución para recibir mensajes
            while True:
                time.sleep(1)

        else:
            print("Error de autenticación.")

    except Exception as e:
        print(f"Error al conectar con EC_Central: {e}")

    finally:
        socket_central.close()
        socket_sensor.close()


if __name__ == "__main__":
    main()