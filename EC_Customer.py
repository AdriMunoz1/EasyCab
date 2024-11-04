import ast
import time
import sys
import json
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

class Customer:
    def __init__(self, ip_broker, port_broker, id_client, initial_position, file_destinations):
        self.id_client = id_client
        self.initial_position = initial_position
        self.file_destinations = file_destinations
        self.producer = KafkaProducer(
            bootstrap_servers=f"{ip_broker}:{port_broker}",
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.consumer = KafkaConsumer(
            f"customer_{self.id_client}_response",
            bootstrap_servers=f"{ip_broker}:{port_broker}",
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=f"customer_{self.id_client}_group",
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def read_services(self):
        """Leer los servicios solicitados desde un archivo en formato (x, y)."""
        services = []
        with open(self.file_destinations, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        destination = ast.literal_eval(line)
                        services.append(destination)
                    except (ValueError, SyntaxError):
                        print("Error al leer el destino en formato (x, y).")
        return services

    def send_request(self, destination):
        """Enviar solicitud de servicio al servidor central en formato JSON."""
        message = {
            "type": "DESTINATION",
            "client_id": self.id_client,
            "destination": destination,
            "initial_position": self.initial_position
        }

        future = self.producer.send("central_requests", message)

        try:
            future.get(timeout=10)
            print(f"Solicitud enviada para el destino: {destination}")
        except KafkaError as e:
            print(f"Error enviando la solicitud: {e}")

    def listen_for_response(self):
        """Esperar respuesta de EC_Central."""
        for message in self.consumer:
            response = message.value  # Ya estÃ¡ deserializado como diccionario
            print(f"Respuesta recibida de EC_Central: {response}")

            if response['status'] == 'OK':
                print(f"Servicio aceptado por EC_Central. Taxi asignado: {response['assigned_taxi']}")
                return True
            elif response['status'] == 'KO':
                print("Servicio denegado por EC_Central.")
                return False

    def run(self):
        """Ejecuta la secuencia de solicitudes de servicio."""
        destinations = self.read_services()

        for destination in destinations:
            self.send_request(destination)
            success = self.listen_for_response()

            if success:
                print(f"Servicio a {destination} completado.")
            else:
                print(f"No se pudo completar el servicio a {destination}.")

            time.sleep(4)

        print("Todas las solicitudes de servicio se han completado.")


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Error: python3 EC_Customer.py <IP Broker> <Port Broker> <ID Client> <File>")
        sys.exit(1)

    ip_broker = sys.argv[1]
    port_broker = int(sys.argv[2])
    id_client = sys.argv[3]
    file_destination = sys.argv[4]
    initial_position = (5, 5)

    customer = Customer(ip_broker, port_broker, id_client, initial_position, file_destination)
    customer.run()

