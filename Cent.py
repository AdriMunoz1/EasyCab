import socket
import sqlite3
import threading
import tkinter as tk

# TAMAÑO MAPA
size = 20
cell_size = 30

def load_city_map(filename):
    city_map = [['' for _ in range(size)] for _ in range(size)]
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

# CONFIGURAR VENTANA GRÁFICA DE TKINTER
root = tk.Tk()
root.title("MAPA")
canvas = tk.Canvas(root, width=size * cell_size, height=size * cell_size)
canvas.pack()

# FUNCION DIBUJAR MAPA
def draw_map(city_map):
    canvas.delete("all")
    for i in range(size):
        for j in range(size):
            x1, y1 = j * cell_size, i * cell_size
            x2, y2 = x1 + cell_size, y1 + cell_size
            color = "gray"
            if city_map[i][j].startswith("T"):
                color = "green" if "verde" in city_map[i][j] else "red"
            elif city_map[i][j] == 'L':
                color = "blue"
            canvas.create_rectangle(x1, y1, x2, y2, fill=color, outline="black")
    root.update_idletasks()

def update_taxi_position(city_map, taxi_id, position, status):
    x, y = position
    city_map[x][y] = f'T{taxi_id} {"verde" if status == "moving" else "rojo"}'
    draw_map(city_map)

# Función para enviar un comando al taxi
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

                if command == "DESTINATION" and extra_param:
                    pos = eval(extra_param)
                    update_taxi_position(city_map, taxi_id, pos, "moving")
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
                    update_taxi_position(city_map, taxi_id, taxi_position, "moving")
                else:
                    response = "KO"
                client_socket.send(response.encode("utf-8"))

            elif request.startswith("STOP#"):
                if taxi_id:
                    update_taxi_position(city_map, taxi_id, taxi_position, "stopped")
                    print(f"Taxi {taxi_id} detenido y marcado como KO")

            elif request.startswith("RESUME#"):
                if taxi_id:
                    update_taxi_position(city_map, taxi_id, taxi_position, "moving")
                    print(f"Taxi {taxi_id} reanudado y marcado como OK")

            elif request.startswith("DESTINATION#"):
                if taxi_id:
                    parts = request.split("#")
                    new_position = eval(parts[2])
                    update_taxi_position(city_map, taxi_id, new_position, "moving")
                    print(f"Taxi {taxi_id} cambiado a destino {new_position}")

            elif request.startswith("RETURN#"):
                if taxi_id:
                    new_position = (1, 1)
                    update_taxi_position(city_map, taxi_id, new_position, "moving")
                    print(f"Taxi {taxi_id} regresando a la base")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if taxi_id:
            client_sockets[:] = [cs for cs in client_sockets if cs["taxi_id"] != taxi_id]
            city_map[taxi_position[0]][taxi_position[1]] = ''
            print(f"Taxi {taxi_id} desconectado.")
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
    threading.Thread(target=run_server, args=(city_map,), daemon=True).start()
    root.mainloop()