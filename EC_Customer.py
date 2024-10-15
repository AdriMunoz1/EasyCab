from confluent_kafka import Producer, Consumer, KafkaError
import time

class ECCustomer:
    def __init__(self, broker, topic, customer_id, destinations_file):
        self.broker = broker
        self.topic = topic
        self.customer_id = customer_id
        self.destinations_file = destinations_file
        self.producer = Producer({'bootstrap.servers': broker})
        self.consumer = Consumer({
            'bootstrap.servers': broker,
            'group.id': f'customer_{customer_id}',
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([f'response_{customer_id}'])

    def delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result. """
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def send_request(self, destination):
        """ Send a taxi service request to the central system """
        message = f'{self.customer_id}#{destination}'
        self.producer.produce(self.topic, message, callback=self.delivery_report)
        self.producer.flush()

    def await_response(self):
        """ Wait for a response from the central system """
        msg = self.consumer.poll(timeout=10.0)
        if msg is None:
            print("No response received within the timeout.")
        elif msg.error():
            print(f'Error: {msg.error()}')
        else:
            print(f"Received response: {msg.value().decode('utf-8')}")

    def process_requests(self):
        """ Process requests from the destinations file """
        with open(self.destinations_file, 'r') as file:
            destinations = file.readlines()

        for destination in destinations:
            destination = destination.strip()
            print(f"Requesting taxi to {destination}")
            self.send_request(destination)
            time.sleep(1)  # Simulate delay
            self.await_response()
            time.sleep(4)  # Wait before the next request


if __name__ == "__main__":
    broker = 'localhost:9092'  # Kafka broker
    topic = 'taxi_requests'
    customer_id = 'customer_1'
    destinations_file = 'destinations.txt'

    customer = ECCustomer(broker, topic, customer_id, destinations_file)
    customer.process_requests()
