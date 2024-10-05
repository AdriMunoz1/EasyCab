import socket
import sqlite3
import threading


def authenticate_taxi(id_taxi):
    connection = sqlite3.connect('taxis.db')
    cursor = connection.cursor()
    cursor.execute('''
                SELECT * FROM taxis WHERE id=?
        ''', (id_taxi,))

    taxi = cursor.fetchone() # Obtener la primera fila del resultado

    connection.close()

    if taxi:
        print(f"Taxi {id_taxi} authenticated successfully!")
        return True
    else:
        print(f"Taxi {id_taxi} failed authentication or is not available.")
        return False

def update_taxi_status(id_taxi, status):
    connection = sqlite3.connect('taxis.db')
    cursor = connection.cursor()

    cursor.execute('''
        UPDATE taxis SET status=? WHERE id=?
    ''', (status, id_taxi))
    connection.commit()
    connection.close()


def handle_client(client_socket, addr):
    try:
        while True:
            request = client_socket.recv(1024).decode("utf-8")
            if not request:
                break
            if request.startswith("AUTH#"):
                taxi_id = request.split("#")[1] #coge lo que está despues del AUTH#
                print(f"Authenticating taxi {taxi_id}")
                #validar si el taxi está en la base de datos
                if authenticate_taxi(taxi_id):
                    #update_taxi_status(int(taxi_id), "OK")
                    response = "OK"
                else:
                    response = "KO"
                client_socket.send(response.encode("utf-8"))
            else:
                print(f"Recieved: {request}")
                response = "Recieved"
                client_socket.send(response.encode("utf-8"))
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client_socket.close()
        print(f"Connection to client ({addr[0]:{addr[1]}}) closed)")

def run_server():
    server_ip = "localhost"
    port = 8000
    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((server_ip, port))
        server_socket.listen()
        print(f"EC_Central listening on {server_ip}:{port}")
        while True:
            client_socket, addr = server_socket.accept()
            print(f"Accepted connection from {addr[0]}:{addr[1]}")
            thread = threading.Thread(target=handle_client, args=(client_socket, addr,))
            thread.start()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        server_socket.close()

if __name__ == "__main__":
    run_server()