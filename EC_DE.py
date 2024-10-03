import socket
import threading
import sys

def get_parameters():
    if len(sys.argv) <= 5:
        print("Usage: python EC_DE  <EC_Central_IP> <EC_Central_Port> <EC_S_IP> <EC_S_Port> <Taxi_ID>")
        sys.exit(1)
    ip_central = sys.argv[1]
    port_central = int(sys.argv[2])
    broker_ip = sys.argv[3]
    broker_port = int(sys.argv[4])
    ip_taxi = sys.argv[5]

    return ip_central, port_central, broker_ip, broker_port, ip_taxi

def validate_taxi(ip_central, port_central, taxi_ip):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((ip_central, port_central))

    msg = f"AUTH#{taxi_ip}"
    client_socket.send(msg.encode("utf-8"))

    response = client_socket.recv(1024).decode("utf-8")
    if response == "OK":
        print("Taxi authenticated successfully!")
        return True
    else:
        print("Authentication failed")
        return False

# Manejo del cliente
def handle_client(client_socket, addr):
    try:
        while True:
            request = client_socket.recv(1024).decode("utf-8")
            if request.lower() == "close":
                client_socket.send("closed".encode("utf-8"))
                break
            print(f"Received: {request}")

            response = "accepted"
            client_socket.send(response.encode("utf-8"))
    except Exception as e:
        print(f"Error when hanlding client: {e}")
    finally:
        client_socket.close()
        print(f"Connection to client ({addr[0]}:{addr[1]}) closed")


def run_server():
    server_ip = "127.0.0.1"
    port = 8000

    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((server_ip, port))
        server.listen()
        print(f"Listening on {server_ip}:{port}")
        while True:
            client_socket, addr = server.accept()
            print(f"Accepted connection from {addr[0]}:{addr[1]}")
            thread = threading.Thread(target=handle_client, args=(client_socket, addr,))
            thread.start()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        server.close()

def main():
    ip_central, port_central, broker_ip, broker_port, ip_taxi = get_parameters()

    if validate_taxi(ip_central, port_central, ip_taxi):
        run_server()
    else:
        msg = input("Do yoy want to register a new taxi? (s/n)")
        if msg.lower() == "s":
            print("Soy gay")


########################## MAIN ##########################
if __name__ == "__main__":
    main()

