import socket
import time
import threading
import sys


# Variable de control global para el envío de OK
PAUSE_TAXI = threading.Event()  # Para pausar y reanudar el envío de OK


# Obtener los parámetros del taxi
def get_parameters():
    if len(sys.argv) <= 2:
        print("Usage: python EC_S.py <IP DE> <Port DE>")
        sys.exit(1)

    ip_de = sys.argv[1]
    port_de = int(sys.argv[2])

    return ip_de, port_de


# Función para enviar mensajes de OK (normal) o KO (incidencia)
def sensor_state(sock, ip_taxi, port_taxi):
    try:
        while True:
            PAUSE_TAXI.wait()  # Espera a que el envío de OK esté permitido
            # Se envía "OK" cada segundo mientras no esté pausado
            sock.sendall(b'OK')
            print("Sensor enviando: OK")
            time.sleep(1)  # Simulación de envío periódico
    except (BrokenPipeError, ConnectionResetError) as e:
        print(f"Error al enviar OK: {e}. Intentando reconectar...")
        reconnect_sensor(sock, ip_taxi, port_taxi)


# Función para manejar el envío del KO
def handle_ko(sock, ip_taxi, port_taxi):
    try:
        while True:
            input("Presiona Enter para simular una indicencia (KO)...")
            sock.sendall(b'KO')  # Enviar "KO" al EC_DE
            print("Sensor enviando: KO. Deteniendo taxi por 5 segundos.")
            PAUSE_TAXI.clear()  # Pausar el envío de OK

            # Esperar 5 segundos antes de continuar
            time.sleep(5)

            # Reanudar automáticamente después de 5 segundos
            sock.sendall(b'RESUME')  # Enviar "RESUME" al EC_DE para reanudar el taxi
            print("Sensor enviando: RESUME. Reanudando taxi.")
            PAUSE_TAXI.set()  # Reanudar el envío de OK

    except (BrokenPipeError, ConnectionResetError) as e:
        print(f"Error al enviar KO: {e}. Intentando reconectar...")
        reconnect_sensor(sock, ip_taxi, port_taxi)


# Función para intentar reconectar si la conexión se pierde
def reconnect_sensor(sock, ip_taxi, port_taxi):
    while True:
        try:
            print(f"Reconectando a {ip_taxi}:{port_taxi}...")
            sock.connect((ip_taxi, port_taxi))
            print(f"Reconexión exitosa a {ip_taxi}:{port_taxi}")
            break
        except Exception as e:
            print(f"Error en la reconexión: {e}")
            time.sleep(3)  # Esperar antes de intentar reconectar nuevamente


# Función principal del sensor EC_S
def main():
    ip_taxi, port_taxi = get_parameters()

    # Crear un socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            # Conectarse al Digital Engine (EC_DE)
            s.connect((ip_taxi, port_taxi))
            print(f"Conectado a EC_DE en {ip_taxi}:{port_taxi}")

            # Iniciar el envío continuo de mensajes OK en un hilo separado
            PAUSE_TAXI.set()  # Permitir inicialmente el envío de OK
            thread_sensor = threading.Thread(target=sensor_state, args=(s, ip_taxi, port_taxi))
            thread_sensor.daemon = True
            thread_sensor.start()

            # Manejar la simulación del KO en el hilo principal
            handle_ko(s, ip_taxi, port_taxi)

        except (BrokenPipeError, ConnectionRefusedError) as e:
            print(f"Error de conexión: {e}. Intentando reconectar...")
            reconnect_sensor(s, ip_taxi, port_taxi)


if __name__ == "__main__":
    main()
