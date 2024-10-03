import socket
import threading

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
                # si encuentra al taxi en la BD
                response = "OK" # Si es exitosa
                # si no
                response = "NOT FOUND"
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
        server_socket.blind((server_ip, port))
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