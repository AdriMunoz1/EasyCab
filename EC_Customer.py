import sys
import time
import json
from kafka import KafkaProducer, KafkaConsumer


def get_parameters():
    if len(sys.argv) != 4:
        print(f"Error: python3 EC_Customer.py <IP Broker> <Port Broker> <ID Client>")
        sys.exit(1)

    ip_broker = sys.argv[1]
    port_broker = int(sys.argv[2])
    id_client = sys.argv[3]

    return ip_broker, port_broker, id_client


def request_taxi(producer, topic, id_client, destination, position_customer):
    message = {
        'customer_id': id_client,
        'destination': destination,
        'position': position_customer
    }
    producer.send(topic, message)
    print(f"Enviando solicitud a EC_Central: {message}")


def get_answer(consumer):
    for message in consumer:
        answer = message.value
        print(f"Respuesta de EC_Central: {answer}")
        return answer


def main():
    ip_broker, port_broker, id_client = get_parameters()
    coor_x = coor_y = 3

    # Configurar productor de Kafka
    topic_producer = 'customer_requests'
    producer = KafkaProducer(bootstrap_servers=f'{ip_broker}:{port_broker}', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # Configurar consumidor de Kafka para recibir respuestas
    topic_consumer = 'customer_responses'
    consumer = KafkaConsumer(
        topic_consumer,
        bootstrap_servers=f'{ip_broker}:{port_broker}',
        group_id=f'CUSTOMER#{id_client}',
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    # Leer archivo con los destinos
    with open('destinos.txt', 'r') as file:
        destinations = file.readlines()

    for destination in destinations:
        position_customer = f"({coor_x}, {coor_y})"
        request_taxi(producer, topic_producer, id_client, destination.strip(), position_customer)
        time.sleep(1)
        answer = get_answer(consumer)

        if answer.get('status') == "OK":
            print("Servicio ACEPTADO. Esperando 4 segundos para la próxima solicitud...")
        else:
            print("Servicio DENEGADO. Esperando 4 segundos para la próxima solicitud...")

        time.sleep(4)  # Esperar 4 segundos antes de la próxima solicitud


if __name__ == "__main__":
    main()
