import socket
import threading
import sys


# Obtener los parámetros pasados por línea de comandos
def get_parameters():
    if len(sys.argv) <= 5:
        print("Usage: python EC_DE <EC_Central_IP> <EC_Central_Port> <EC_S_IP> <EC_S_Port> <Taxi_ID>")
        sys.exit(1)
    ip_central = sys.argv[1]
    port_central = int(sys.argv[2])
    broker_ip = sys.argv[3]
    broker_port = int(sys.argv[4])
    taxi_id = sys.argv[5]

    return ip_central, port_central, broker_ip, broker_port, taxi_id


# Validar el taxi con EC_Central
def validate_taxi(ip_central, port_central, taxi_id):
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((ip_central, port_central))

        msg = f"AUTH#{taxi_id}"
        client_socket.send(msg.encode("utf-8"))

        response = client_socket.recv(1024).decode("utf-8")
        print(f"Respuesta de EC_Central: {response}")

        if response == "OK":
            print("Taxi authenticated successfully!")
            client_socket.close()
            return True
        else:
            print("Authentication failed")
            client_socket.close()
            return False
    except Exception as e:
        print(f"Error al autenticar el taxi: {e}")
        return False


# Manejo de las solicitudes de los clientes de EC_S
def handle_client(client_socket, addr):
    try:
        while True:
            request = client_socket.recv(1024).decode("utf-8")
            if request.lower() == "close":
                client_socket.send("closed".encode("utf-8"))
                break
            print(f"Received from sensor {addr}: {request}")

            response = "accepted"
            client_socket.send(response.encode("utf-8"))
    except Exception as e:
        print(f"Error handling client {addr}: {e}")
    finally:
        client_socket.close()
        print(f"Connection to client ({addr[0]}:{addr[1]}) closed")


# Escuchar solicitudes de EC_S en un servidor
def run_server(taxi_id):
    server_ip = "localhost"
    port = 8001 + int(taxi_id)  # Usar el ID del taxi para asignar un puerto único

    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((server_ip, port))
        server.listen()
        print(f"Taxi listening on {server_ip}:{port}")

        while True:
            client_socket, addr = server.accept()
            print(f"Accepted connection from {addr[0]}:{addr[1]}")
            thread = threading.Thread(target=handle_client, args=(client_socket, addr,))
            thread.start()
    except Exception as e:
        print(f"Error en el servidor del taxi: {e}")
    finally:
        server.close()


# Función para gestionar la entrada del usuario para detener el taxi
def listen_for_user_input(central_socket, taxi_id, server_socket):
    while True:
        command = input("Introduce 'stop' para desconectar el taxi: ")
        if command.lower() == "stop":
            disconnect_taxi(central_socket, taxi_id)
            shutdown_taxi_server(server_socket)
            break


# Función para desconectar el taxi de EC_Central
def disconnect_taxi(central_socket, taxi_id):
    message = f"DISCONNECT#{taxi_id}"
    central_socket.send(message.encode('utf-8'))
    print(f"Taxi {taxi_id} desconectado de EC_Central.")
    central_socket.close()


# Cerrar el servidor del taxi
def shutdown_taxi_server(server_socket):
    print("Apagando el servidor del taxi...")
    server_socket.close()

def handle_central_commands(central_socket):
    try:
        while True:
            message = central_socket.recv(1024).decode("utf-8")
            if message.startswith("STOP#"):
                print("Recibido comando para detener el taxi")
                # Aquí puedes implementar la lógica para detener el taxi
            elif message.startswith("RESUME#"):
                print("Recibido comando para reanudar el taxi")
                # Aquí puedes implementar la lógica para reanudar el taxi
            elif message.startswith("DESTINATION#"):
                parts = message.split("#")
                new_destination = parts[2]
                print(f"Recibido nuevo destino: {new_destination}")
                # Aquí puedes implementar la lógica para cambiar el destino del taxi
            elif message.startswith("RETURN#"):
                print("Recibido comando para volver a la base [1,1]")
                # Aquí puedes implementar la lógica para que el taxi vuelva a la base
    except Exception as e:
        print(f"Error al recibir comandos: {e}")


# Función principal del taxi
def main():
    ip_central, port_central, broker_ip, broker_port, taxi_id = get_parameters()

    # Validar el taxi con EC_Central
    if validate_taxi(ip_central, port_central, taxi_id):
        # Ejecutar el servidor del taxi para escuchar señales de EC_S
        server_thread = threading.Thread(target=run_server, args=(taxi_id,))
        server_thread.start()

        # Simular la conexión con EC_Central
        central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        central_socket.connect((ip_central, port_central))

        central_command_thread = threading.Thread(target=handle_central_commands, args=(central_socket,))
        central_command_thread.start()

        # Escuchar comandos del usuario para desconectar el taxi
        listen_for_user_input(central_socket, taxi_id, server_thread)

    else:
        print("Autenticación fallida. Por favor, intenta nuevamente.")


########################## MAIN ##########################
if __name__ == "__main__":
    main()
