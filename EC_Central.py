import socket
import sqlite3
import threading


# Función para actualizar el estado de un taxi en la base de datos
def update_taxi_status(status, id_taxi):
    try:
        connection = sqlite3.connect('taxis.db')
        cursor = connection.cursor()

        # Actualizar el estado del taxi (OK o KO)
        cursor.execute('''
            UPDATE taxis SET estado = ? WHERE id = ?
        ''', (status, id_taxi))
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

        # Verificar si el taxi existe en la base de datos
        cursor.execute('''
            SELECT * FROM taxis WHERE id = ?
        ''', (id_taxi,))

        taxi = cursor.fetchone()
        connection.close()

        if taxi:
            print(f"Taxi {id_taxi} autenticado correctamente.")
            # Cambiar el estado del taxi a "OK"
            update_taxi_status("OK", int(id_taxi))
            return True
        else:
            print(f"Taxi {id_taxi} no está registrado.")
            return False
    except sqlite3.Error as e:
        print(f"Error al autenticar el taxi: {e}")
        return False


# Función para manejar las conexiones con los taxis
def handle_client(client_socket, addr):
    taxi_id = None
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
                else:
                    response = "KO"
                client_socket.send(response.encode("utf-8"))

            elif request.startswith("STOP#"):
                if taxi_id:
                    client_socket.send(f"STOP#{taxi_id}".encode("utf-8"))
                    update_taxi_status("KO", taxi_id)
                    print(f"Taxi {taxi_id} stopped")
                else:
                    response = "Taxi not authenticated"
                    client_socket.send(response.encode("utf-8"))

            elif request.startswith("RESUME#"):
                if taxi_id:
                    client_socket.send(f"RESUME#{taxi_id}".encode("utf-8"))
                    update_taxi_status("OK", taxi_id)
                    print(f"Taxi {taxi_id} resumed")
                else:
                    response = "Taxi not authenticated"
                    client_socket.send(response.encode("utf-8"))

            elif request.startswith("DESTINATION#"):
                if taxi_id:
                    new_destination = request.split("#")[1]
                    client_socket.send(f"DESTINATION#{taxi_id}#{new_destination}".encode("utf-8"))
                    print(f"Taxi {taxi_id} destination changed to {new_destination}")
                else:
                    response = "Taxi not authenticated"
                    client_socket.send(response.encode("utf-8"))

            elif request.startswith("RETURN#"):
                if taxi_id:
                    client_socket.send(f"RETURN#{taxi_id}".encode("utf-8"))
                    print(f"Taxi {taxi_id} returning to base")
                else:
                    response = "Taxi not authenticated"
                    client_socket.send(response.encode("utf-8"))

            elif request.startswith("DISCONNECT#"):
                taxi_id = request.split("#")[1]
                print(f"Taxi {taxi_id} desconectado y marcado como KO.")
                update_taxi_status("KO", int(taxi_id))
                break

            else:
                print(f"Recibido de {addr}: {request}")
                response = "Recibido"
                client_socket.send(response.encode("utf-8"))

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if taxi_id:
            update_taxi_status("KO", taxi_id)
            print(f"Taxi {taxi_id} desconectado y marcado como KO.")
            client_socket.close()
            print(f"Conexión con {addr[0]}:{addr[1]} cerrada.")


# Función para ejecutar el servidor EC_Central
def run_server():
    server_ip = "localhost"
    port = 8000
    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((server_ip, port))
        server_socket.listen()
        print(f"EC_Central escuchando en {server_ip}:{port}")

        while True:
            client_socket, addr = server_socket.accept()
            print(f"Conexión aceptada de {addr[0]}:{addr[1]}")
            thread = threading.Thread(target=handle_client, args=(client_socket, addr,))
            thread.start()

    except Exception as e:
        print(f"Error en el servidor: {e}")
    finally:
        server_socket.close()


# Ejecutar el servidor EC_Central
if __name__ == "__main__":
    run_server()
