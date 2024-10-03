import socket
import time

class Customer:
    def __init__(self, customer_id, central_ip, central_port, request_file):
        self.customer_id = customer_id
        self.central_ip = central_ip
        self.central_port = central_port
        self.request_file = request_file
        self.socket = None
        self.connected = False

    def connect_to_central(self):
        """Conectar el cliente a la central."""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.central_ip, self.central_port))
            self.connected = True
            print(f"Cliente {self.customer_id} conectado a la central en {self.central_ip}:{self.central_port}")
            self.authenticate()
        except Exception as e:
            print(f"Error conectando el cliente {self.customer_id}: {e}")

    def authenticate(self):
        """Autenticarse con la central."""
        auth_message = f"AUTH#Customer#{self.customer_id}"
        self.socket.sendall(auth_message.encode())
        print(f"Cliente {self.customer_id} autenticado.")
        # La central debería responder con ACK si la autenticación es correcta.

    def send_service_request(self, destination):
        """Enviar una solicitud de servicio a la central."""
        request_message = f"REQUEST#{self.customer_id}#{destination}"
        self.socket.sendall(request_message.encode())
        print(f"Cliente {self.customer_id} envió una solicitud para el destino {destination}")

        # Esperar la respuesta de la central
        response = self.socket.recv(1024).decode()
        if response == "OK":
            print(f"Cliente {self.customer_id} recibió confirmación de servicio: OK")
        elif response == "KO":
            print(f"Cliente {self.customer_id} recibió denegación de servicio: KO")

    def process_requests(self):
        """Procesar las solicitudes desde el archivo."""
        with open(self.request_file, 'r') as file:
            destinations = [line.strip() for line in file.readlines()]

        for destination in destinations:
            self.send_service_request(destination)
            # Esperar 4 segundos entre solicitudes
            time.sleep(4)

    def disconnect(self):
        """Cerrar la conexión con la central."""
        if self.socket:
            self.socket.close()
        print(f"Cliente {self.customer_id} desconectado.")

if __name__ == "__main__":
    # Parámetros de configuración
    customer_id = 1
    central_ip = "127.0.0.1"
    central_port = 12345
    request_file = "customer_requests.txt"  # Archivo que contiene los destinos de los viajes

    # Crear y ejecutar el cliente
    customer = Customer(customer_id, central_ip, central_port, request_file)
    customer.connect_to_central()
    customer.process_requests()
    customer.disconnect()
