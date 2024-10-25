import socket
import sqlite3
import threading

def load_city_map(filename):
    city_map = [['' for _ in range(20)] for _ in range(20)]
    try:
        with open(filename, 'r') as file:
            for line in file:
                parts = line.strip().split()
                if len(parts) == 3:
                    id_localizacion = parts[0]
                    coord_x = int(parts[1])
                    coord_y = int(parts[2])
                    city_map[coord_x][coord_y] = id_localizacion
        print("Mapa de la ciudad cargado correctamente.")
    except Exception as e:
        print(f"Error al cargar el mapa de la ciudad: {e}")
    return city_map

# Función para enviar un comando al taxi (STOP, RESUME, DESTINATION, RETURN)
def send_command(client_sockets, command, taxi_id, extra_param=None):
    try:
        for client_socket in client_sockets:
            if client_socket["taxi_id"] == taxi_id:
                if command == "DESTINATION":
                    msg = f"{command}#{taxi_id}#{extra_param}"
                else:
                    msg = f"{command}#{taxi_id}"

                client_socket["socket"].send(msg.encode('utf-8'))
                print(f"Comando {command} enviado al taxi {taxi_id}")

                # Recibir respuesta del taxi
                mensaje = client_socket["socket"].recv(1024).decode('utf-8')
                if mensaje == 'OK' and command == 'STOP':
                    update_taxi_status("KO", taxi_id)
                elif mensaje == 'OK' and command == 'RESUME':
                    update_taxi_status("OK", taxi_id)
                return
        print(f"Taxi {taxi_id} no encontrado.")
    except Exception as e:
        print(f"Error al enviar el comando {command} al taxi {taxi_id}: {e}")

# Hilo para manejar los comandos del usuario
def command_input_handler(client_sockets):
    while True:
        command = input("Introduce un comando (STOP/RESUME/DESTINATION/RETURN) para el taxi: ")
        taxi_id = input("Introduce el ID del taxi: ")

        if command in ["STOP", "RESUME"]:
            send_command(client_sockets, command, taxi_id)
        elif command == "DESTINATION":
            new_destination = input("Introduce la nueva localización (ID_LOCALIZACION): ")
            send_command(client_sockets, "DESTINATION", taxi_id, new_destination)
        elif command == "RETURN":
            send_command(client_sockets, "RETURN", taxi_id)
        else:
            print("Comando no válido. Usa STOP, RESUME, DESTINATION o RETURN.")

# Función para actualizar el estado de un taxi en la base de datos
def update_taxi_status(status, id_taxi):
    try:
        connection = sqlite3.connect('taxis.db')
        cursor = connection.cursor()
        cursor.execute('UPDATE taxis SET estado = ? WHERE id = ?', (status, id_taxi))
        connection.commit()
        connection.close()
        print(f"Taxi {id_taxi} actualizado a {status}.")
    except sqlite3.Error as e:
        print(f"Error al actualizar el estado del taxi {id_taxi}: {e}")

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

# Función para manejar las conexiones con los taxis
def handle_client(client_socket, addr, city_map, client_sockets):
    taxi_id = None
    taxi_position = None
    try:
        while True:
            request = client_socket.recv(1024).decode("utf-8")
            if not request:
                break

            if request.startswith("AUTH#"):
                taxi_id = request.split("#")[1]
                print(f"Autenticando taxi {taxi_id}")

                if authenticate_taxi(taxi_id):
                    response = "OK"
                    client_sockets.append({"socket": client_socket, "taxi_id": taxi_id})
                    taxi_position = (1, 1)
                    city_map[taxi_position[0]][taxi_position[1]] = f'T{taxi_id} verde'
                else:
                    response = "KO"
                client_socket.send(response.encode("utf-8"))

            elif request.startswith("STOP#"):
                if taxi_id:
                    city_map[taxi_position[0]][taxi_position[1]] = f'T{taxi_id} rojo'
                    client_socket.send(f"STOP#{taxi_id}".encode("utf-8"))
                    update_taxi_status("KO", taxi_id)
                    print(f"Taxi {taxi_id} detenido y marcado como KO")
                else:
                    response = "Taxi no autenticado"
                    client_socket.send(response.encode("utf-8"))

            elif request.startswith("RESUME#"):
                if taxi_id:
                    city_map[taxi_position[0]][taxi_position[1]] = f'T{taxi_id} verde'
                    client_socket.send(f"RESUME#{taxi_id}".encode("utf-8"))
                    update_taxi_status("OK", taxi_id)
                    print(f"Taxi {taxi_id} reanudado y marcado como OK")
                else:
                    response = "Taxi no autenticado"
                    client_socket.send(response.encode("utf-8"))

            elif request.startswith("DESTINATION#"):
                if taxi_id:
                    id_localizacion = request.split("#")[2]
                    for x in range(20):
                        for y in range(20):
                            if city_map[x][y] == id_localizacion:
                                new_destination = (x, y)
                                if taxi_position:
                                    city_map[taxi_position[0]][taxi_position[1]] = ''
                                taxi_position = new_destination
                                city_map[new_destination[0]][new_destination[1]] = f'T{taxi_id} verde'
                                client_socket.send(f"DESTINATION#{taxi_id}#{new_destination}".encode("utf-8"))
                                print(f"Taxi {taxi_id} cambiado a destino {new_destination}")
                                break
                else:
                    response = "Taxi no autenticado"
                    client_socket.send(response.encode("utf-8"))

            elif request.startswith("RETURN#"):
                if taxi_id:
                    if taxi_position:
                        city_map[taxi_position[0]][taxi_position[1]] = ''
                    taxi_position = (1, 1)
                    city_map[1][1] = f'T{taxi_id} verde'
                    client_socket.send(f"RETURN#{taxi_id}".encode("utf-8"))
                    print(f"Taxi {taxi_id} regresando a la base")
                else:
                    response = "Taxi no autenticado"
                    client_socket.send(response.encode("utf-8"))

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if taxi_id:
            update_taxi_status("KO", taxi_id)
            client_sockets[:] = [cs for cs in client_sockets if cs["taxi_id"] != taxi_id]
            if taxi_position:
                city_map[taxi_position[0]][taxi_position[1]] = ''
            print(f"Taxi {taxi_id} desconectado y marcado como KO.")
            client_socket.close()
            print(f"Conexión con {addr[0]}:{addr[1]} cerrada.")

# Función para ejecutar el servidor EC_Central
def run_server(city_map):
    server_ip = "localhost"
    port = 8000
    client_sockets = []
    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((server_ip, port))
        server_socket.listen()
        print(f"EC_Central escuchando en {server_ip}:{port}")

        threading.Thread(target=command_input_handler, args=(client_sockets,)).start()

        while True:
            client_socket, addr = server_socket.accept()
            print(f"Conexión aceptada de {addr[0]}:{addr[1]}")
            thread = threading.Thread(target=handle_client, args=(client_socket, addr, city_map, client_sockets))
            thread.start()

    except Exception as e:
        print(f"Error en el servidor: {e}")
    finally:
        server_socket.close()

# Ejecutar el servidor EC_Central
if __name__ == "__main__":
    city_map = load_city_map('city_map.txt')
    run_server(city_map)
