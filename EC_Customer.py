import sys
import time
from kafka import KafkaProducer, KafkaConsumer


def get_parameters():
    if len(sys.argv) != 4:
        print(f"Error: python3 EC_Customer.py <IP Broker> <Port Broker>  <ID Client>")
        sys.exit(1)

    ip_broker = sys.argv[1]
    port_broker = int(sys.argv[2])
    id_client = sys.argv[3]

    return ip_broker, port_broker, id_client


def request_taxi(producer, topic, id_client, destination):
    message = f"{id_client}#{destination}"
    producer.send(topic, message.encode('utf-8'))
    print(f"Enviando solicitud a EC_Central: {message}")


def get_answer(consumer):
    for message in consumer:
        answer = message.value.decode('utf-8')
        print(f"Respuesta de EC_Central: {answer}")
        return answer


def main():
    ip_broker, port_broker, id_client = get_parameters()

    # Configurar productor de Kafka
    topic_producer = 'solicitud_central'
    producer = KafkaProducer(bootstrap_servers=f'{ip_broker}:{port_broker}')

    # Configurar consumidor de Kafka para recibir respuestas
    topic_consumer = 'respuesta_central'
    consumer = KafkaConsumer(
        topic_consumer,
        bootstrap_servers=f'{ip_broker}:{port_broker}',
        group_id=f'{id_client}',
        auto_offset_reset='earliest'
    )

    # Leer archivo con los destinos
    with open('destinos.txt', 'r') as file:
        destinations = file.readlines()

    for destination in destinations:
        request_taxi(producer, topic_producer, id_client, destination.strip())
        answer = get_answer(consumer)

        if answer == "OK":
            print("Servicio ACEPTADO. Esperando 4 segundos para la próxima solicitud...")
        else:
            print("Servicio DENEGADO. Esperando 4 segundos para la próxima solicitud...")

        time.sleep(4)  # Esperar 4 segundos antes de la próxima solicitud


######################### MAIN #########################
if __name__ == "__main__":
    main()