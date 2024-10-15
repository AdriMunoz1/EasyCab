import socket
import time
import threading
import sys
# IP y puerto del EC_DE al que se conectará EC_S (el Digital Engine del taxi)
EC_DE_IP = '127.0.0.1'  # Cambiar por la IP real del EC_DE
EC_DE_PORT = 8001  # Cambiar por el puerto real del EC_DE

def get_parametres():
    if len(sys.argv) <= 2:
        print("Usage: python ip y puerto EC_DE")
        sys.exit(1)
    ip_taxi = sys.argv[1]
    port_taxi = int(sys.argv[2])
    return ip_taxi, port_taxi

# Función para enviar mensajes de OK (normal) o KO (incidencia)
def sensor_state(sock):
    while True:
        # Se envía "OK" cada segundo para indicar que todo está bien
        sock.sendall(b'OK')
        print("Sensor enviando: OK")
        time.sleep(1)

def main():

    ip_taxi, port_taxi = get_parametres()
    # Crear un socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # Conectarse al Digital Engine (EC_DE)
        s.connect((ip_taxi, port_taxi))
        print(f"Conectado a EC_DE en {EC_DE_IP}:{EC_DE_PORT}")

        # Iniciar el envío continuo de mensajes OK
        sensor_thread = threading.Thread(target=sensor_state, args=(s,))
        sensor_thread.daemon = True
        sensor_thread.start()

        # Esperar interacciones del usuario para simular un KO
        while True:
            input("Presiona Enter para simular una incidencia (KO)...")
            s.sendall(b'KO')  # Enviar "KO" al EC_DE
            print("Sensor enviando: KO")
            time.sleep(1)  # Esperar un segundo antes de reanudar

if __name__ == "__main__":
    main()
