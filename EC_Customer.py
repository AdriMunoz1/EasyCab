import sys
import time
from kafka import KafkaProducer, KafkaConsumer

def get_parameters():
    if len(sys.argv) <= 3:
        print(f"Error: python3 EC_Customer.py <IP Broker> <Port Broker>  <ID Client>")
        sys.exit(1)

    ip_broker = sys.argv[1]
    port_broker = int(sys.argv[2])
    id_client = sys.argv[3]

    return ip_broker, port_broker, id_client


def request_taxi(producer, topic, id_client, destiny):
    message = f"{id_client}#{destiny}"
    producer.send(topic, message.encode('utf-8'))
    print(f"Enviando solicitud: {message}")


def get_answer(consumer):
    for message in consumer:
        answer = message.value.decode('utf-8')
        print(f"Respuesta de EC_Central: {answer}")
        return answer


def main():
    ip_broker, port_broker, id_client = get_parameters()

    # Configurar productor de Kafka
    producer = KafkaProducer(bootstrap_servers=f'{ip_broker}:{port_broker}')
    topic_request = 'central_topic'

    # Configurar consumidor de Kafka para recibir respuestas
    consumer = KafkaConsumer(
        'respuesta_central',
        bootstrap_servers=f'{ip_broker}:{port_broker}',
        group_id=f"grupo_cliente_{id_client}",
        auto_offset_reset='earliest'
    )

    # Leer archivo con los destinos
    with open('destinos.txt', 'r') as file:
        destinies = file.readlines()

    for destiny in destinies:
        request_taxi(producer, topic_request, id_client, destiny.strip())
        respuesta = get_answer(consumer)

        if respuesta == "OK":
            print("Servicio aceptado. Esperando 4 segundos para la próxima solicitud...")
        else:
            print("Servicio denegado. Esperando 4 segundos para la próxima solicitud...")

        time.sleep(4)  # Esperar 4 segundos antes de la próxima solicitud


######################### MAIN #########################
if __name__ == "__main__":
    main()