import socket
import threading
import sys
import time
from kafka import KafkaProducer, KafkaConsumer
import json
import signal

# Configuración de Kafka
KAFKA_SERVER = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Variables globales para manejar el estado del taxi
taxi_status_lock = threading.Lock()
taxi_position = None
destination_position = None
taxi_id = None
stop_event = threading.Event()
last_ok_time = time.time()  # Tiempo del último OK recibido
is_paused = False

# Obtener los parámetros pasados por línea de comandos
def get_parameters():
    if len(sys.argv) != 7:
        print("Uso: python EC_DE.py <EC_Central_IP> <Port_Central> <IP_Broker> <Port_Broker> <IP_S> <ID_Taxi>")
        sys.exit(1)
    ip_central = sys.argv[1]
    port_central = int(sys.argv[2])
    ip_broker = sys.argv[3]
    port_broker = int(sys.argv[4])
    ip_s = sys.argv[5]
    global taxi_id
    taxi_id = sys.argv[6]
    return ip_central, port_central, ip_broker, port_broker, ip_s

# Validar el taxi con EC_Central usando sockets
def validate_taxi_with_central(ip_central, port_central):
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((ip_central, port_central))

        # Enviar solicitud de autenticación
        msg = f"AUTH#{taxi_id}"
        client_socket.send(msg.encode('utf-8'))

        # Recibir respuesta de autenticación
        response = client_socket.recv(1024).decode('utf-8')
        if response == "OK":
            print(f"[Autenticación] Taxi {taxi_id} autenticado correctamente con EC_Central.")
            return True
        else:
            print(f"[Autenticación] Falló la autenticación para el taxi {taxi_id}.")
            return False
    except Exception as e:
        print(f"[Error] Error al autenticar el taxi con EC_Central: {e}")
        return False

# Enviar la posición actual a Kafka
def send_position_to_kafka():
    if is_paused:
        return  # No enviar la posición si el taxi está detenido
    position_topic = 'taxi_updates'
    msg = {
        'taxi_id': taxi_id,
        'status': 'moving',
        'position': taxi_position
    }
    producer.send(position_topic, msg)
    producer.flush()
    print(f"Posición enviada a Kafka: {msg}")



# Notificar llegada al destino
def notify_arrival():
    arrival_topic = 'taxi_updates'
    msg = {
        'taxi_id': taxi_id,
        'status': 'arrived',
        'position': taxi_position
    }
    producer.send(arrival_topic, msg)
    producer.flush()
    print(f"Taxi {taxi_id} ha llegado al destino {taxi_position}")
"""
# Función para mover el taxi hacia el destino
def move_taxi_to_destination():
    global taxi_position, destination_position

    if taxi_position is None:
        # Inicializar la posición del taxi si aún no se ha hecho
        taxi_position = (1, 1)

    if destination_position is None:
        print("Error: El destino no ha sido establecido. Posiciones no inicializadas correctamente.")
        return

    while taxi_position != destination_position:
        # Verificar si se ha recibido una señal de detener
        if stop_event.is_set():
            print(f"Taxi {taxi_id} detenido en {taxi_position}")
            return  # Salir de la función para detener el movimiento

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

        # Enviar la nueva posición a Kafka
        send_position_to_kafka()
        print(f"Taxi {taxi_id} moviéndose a {taxi_position}")

    # Si el taxi no fue detenido, ha llegado al destino
    if taxi_position == destination_position:
        print(f"Taxi {taxi_id} ha llegado a su destino: {destination_position}")
        notify_arrival()
"""


# Función para mover el taxi hacia el destino
def move_taxi_to_destination():
    global taxi_position, destination_position

    if taxi_position is None:
        # Inicializar la posición del taxi si aún no se ha hecho
        taxi_position = (1, 1)

    if destination_position is None:
        print("Error: El destino no ha sido establecido. Posiciones no inicializadas correctamente.")
        return

    while taxi_position != destination_position:
        # Verificar si se ha recibido una señal de detener ANTES de cualquier movimiento
        if stop_event.is_set():
            print(f"Taxi {taxi_id} detenido en {taxi_position}")

            # Enviar actualización del estado a Kafka para indicar que está detenido
            update_topic = 'taxi_updates'
            msg = {
                'taxi_id': taxi_id,
                'status': 'stopped',
                'position': taxi_position  # Enviar la posición actual
            }
            producer.send(update_topic, msg)
            producer.flush()
            print(f"Estado 'stopped' enviado a Kafka para taxi {taxi_id}")
            return  # Salir de la función para detener el movimiento

        # Actualizar posición en eje X
        if taxi_position[0] < destination_position[0]:
            taxi_position = (taxi_position[0] + 1, taxi_position[1])
        elif taxi_position[0] > destination_position[0]:
            taxi_position = (taxi_position[0] - 1, taxi_position[1])

        # Verificar nuevamente si hay una señal de detener
        if stop_event.is_set():
            print(f"Taxi {taxi_id} detenido en {taxi_position}")

            # Enviar actualización del estado a Kafka para indicar que está detenido
            update_topic = 'taxi_updates'
            msg = {
                'taxi_id': taxi_id,
                'status': 'stopped',
                'position': taxi_position  # Enviar la posición actual
            }
            producer.send(update_topic, msg)
            producer.flush()
            print(f"Estado 'stopped' enviado a Kafka para taxi {taxi_id}")
            return  # Salir de la función para detener el movimiento

        # Actualizar posición en eje Y
        if taxi_position[1] < destination_position[1]:
            taxi_position = (taxi_position[0], taxi_position[1] + 1)
        elif taxi_position[1] > destination_position[1]:
            taxi_position = (taxi_position[0], taxi_position[1] - 1)

        # Simular el movimiento con un retardo
        time.sleep(1)

        # Enviar la nueva posición a Kafka
        send_position_to_kafka()
        print(f"Taxi {taxi_id} moviéndose a {taxi_position}")

    # Si el taxi no fue detenido, ha llegado al destino
    if taxi_position == destination_position:
        print(f"Taxi {taxi_id} ha llegado a su destino: {destination_position}")
        notify_arrival()


# Manejar la comunicación con EC_S usando sockets
def run_sensor_server(ip_s):
    global last_ok_time, is_paused
    port = 8000 + int(taxi_id)  # El puerto para cada taxi será 8000 + ID del taxi
    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((ip_s, port))
        server_socket.listen(1)
        print(f"Servidor de sensores escuchando en {ip_s}:{port}")

        while True:
            client_socket, addr = server_socket.accept()
            print(f"Conexión de EC_S aceptada desde {addr}")
            try:
                while True:
                    data = client_socket.recv(1024).decode('utf-8')
                    if not data:
                        break
                    if data == "OK":
                        last_ok_time = time.time()  # Actualizar el tiempo del último OK recibido
                        print(f"[Sensor] Recibido 'OK' de EC_S para el taxi {taxi_id}")
                        stop_event.clear()  # Reanudar movimiento si se está recibiendo 'OK'
                        is_paused = False
                    elif data == "KO":
                        print(f"[Sensor] Recibido 'KO' de EC_S para el taxi {taxi_id}")
                        stop_event.set()  # Pausar el taxi
                        is_paused = True
                        send_stop_command()  # Enviar comando STOP a EC_Central
                        time.sleep(5)  # Esperar 5 segundos
                        send_resume_command()  # Enviar comando RESUME a EC_Central
            except Exception as e:
                print(f"[Error] Error en la comunicación con EC_S: {e}")
            finally:
                client_socket.close()
                print(f"Conexión con EC_S cerrada.")
    except Exception as e:
        print(f"[Error] Error en el servidor de sensores: {e}")



# Enviar comando STOP a EC_Central
def send_stop_command():
    msg = {
        'taxi_id': taxi_id,
        'command': 'STOP'
    }
    producer.send('taxi_commands', msg)
    producer.flush()
    print(f"Comando STOP enviado al taxi {taxi_id} a través de Kafka")

# Enviar comando RESUME a EC_Central
def send_resume_command():
    msg = {
        'taxi_id': taxi_id,
        'command': 'RESUME'
    }
    producer.send('taxi_commands', msg)
    producer.flush()
    print(f"Comando RESUME enviado al taxi {taxi_id} a través de Kafka")
"""
# Manejo de comandos desde Kafka
def receive_commands():
    global destination_position

    command_topic = 'taxi_commands'
    consumer = KafkaConsumer(
        command_topic,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id=f'taxi_{taxi_id}_commands',
        auto_offset_reset='earliest'
    )

    try:
        for message in consumer:
            command = message.value.get('command')
            received_taxi_id = message.value.get('taxi_id')
            if received_taxi_id == taxi_id:
                if command == "STOP":
                    stop_event.set()
                    print(f"Taxi {taxi_id} detenido por comando STOP")

                elif command == "RESUME":
                    stop_event.clear()
                    print(f"Taxi {taxi_id} reanudado por comando RESUME")
                    # Continuar movimiento si el taxi estaba detenido
                    threading.Thread(target=move_taxi_to_destination, daemon=True).start()

                elif command == "DESTINATION":
                    new_destination = message.value.get('extra_param')
                    try:
                        # Asegurar que el destino sea una tupla válida
                        destination_position = eval(new_destination)
                        if isinstance(destination_position, tuple) and len(destination_position) == 2:
                            stop_event.clear()
                            print(f"Recibido nuevo destino para el taxi {taxi_id}: {destination_position}")
                            threading.Thread(target=move_taxi_to_destination, daemon=True).start()
                        else:
                            print(f"Error: Formato de destino inválido recibido: {new_destination}")
                    except Exception as e:
                        print(f"Error al establecer el destino: {e}")

                elif command == "RETURN":
                    stop_event.clear()
                    destination_position = (1, 1)
                    print(f"Recibido comando para volver a la base para el taxi {taxi_id}")
                    threading.Thread(target=move_taxi_to_destination, daemon=True).start()
    except Exception as e:
        print(f"Error al recibir comandos de Kafka: {e}")
"""

# Manejo de comandos desde Kafka
def receive_commands():
    global destination_position

    command_topic = 'taxi_commands'
    consumer = KafkaConsumer(
        command_topic,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id=f'taxi_{taxi_id}_commands',
        auto_offset_reset='earliest'
    )

    try:
        for message in consumer:
            command = message.value.get('command')
            received_taxi_id = message.value.get('taxi_id')
            if received_taxi_id == taxi_id:
                if command == "STOP":
                    stop_event.set()
                    print(f"Taxi {taxi_id} detenido por comando STOP")

                    # Enviar actualización del estado a Kafka para indicar que está detenido
                    update_topic = 'taxi_updates'
                    msg = {
                        'taxi_id': taxi_id,
                        'status': 'stopped',
                        'position': taxi_position  # Enviar la posición actual
                    }
                    producer.send(update_topic, msg)
                    producer.flush()
                    print(f"Estado 'stopped' enviado a Kafka para taxi {taxi_id}")

                elif command == "RESUME":
                    stop_event.clear()
                    print(f"Taxi {taxi_id} reanudado por comando RESUME")
                    # Continuar movimiento si el taxi estaba detenido
                    threading.Thread(target=move_taxi_to_destination, daemon=True).start()

                elif command == "DESTINATION":
                    new_destination = message.value.get('extra_param')
                    try:
                        # Asegurar que el destino sea una tupla válida
                        destination_position = eval(new_destination)
                        if isinstance(destination_position, tuple) and len(destination_position) == 2:
                            stop_event.clear()
                            print(f"Recibido nuevo destino para el taxi {taxi_id}: {destination_position}")
                            threading.Thread(target=move_taxi_to_destination, daemon=True).start()
                        else:
                            print(f"Error: Formato de destino inválido recibido: {new_destination}")
                    except Exception as e:
                        print(f"Error al establecer el destino: {e}")

                elif command == "RETURN":
                    stop_event.clear()
                    destination_position = (1, 1)
                    print(f"Recibido comando para volver a la base para el taxi {taxi_id}")
                    threading.Thread(target=move_taxi_to_destination, daemon=True).start()
    except Exception as e:
        print(f"Error al recibir comandos de Kafka: {e}")


# Manejo de la desconexión del taxi
def handle_exit_signal(signal_received, frame):
    print(f"\n[Desconexión] Taxi {taxi_id} se está desconectando...")
    # Enviar actualización del estado a 'KO' a través de Kafka
    update_topic = 'taxi_updates'
    msg = {
        'taxi_id': taxi_id,
        'status': 'KO'
    }
    producer.send(update_topic, msg)
    producer.flush()
    print(f"[Desconexión] Estado del taxi {taxi_id} actualizado a 'KO'.")
    sys.exit(0)

# Función principal del taxi
def main():
    signal.signal(signal.SIGINT, handle_exit_signal)  # Capturar la señal Ctrl+C para manejar la desconexión

    ip_central, port_central, ip_broker, port_broker, ip_s = get_parameters()

    # Validar el taxi usando sockets con EC_Central
    if not validate_taxi_with_central(ip_central, port_central):
        sys.exit(1)

    # Iniciar un hilo para manejar la conexión con EC_S
    sensor_thread = threading.Thread(target=run_sensor_server, args=(ip_s,), daemon=True)
    sensor_thread.start()

    # Iniciar un hilo para recibir comandos de Kafka
    command_thread = threading.Thread(target=receive_commands, daemon=True)
    command_thread.start()

    # Mantener el programa corriendo
    command_thread.join()

if __name__ == "__main__":
    main()
