import logging

from sseclient import SSEClient as EventSource
from confluent_kafka import Producer

logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ProducerDemo:
    STREAMS_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

    def __init__(self):
        self.producer = None

    def create_producer(self):
        self.producer = Producer({
            "bootstrap.servers": "127.0.0.1:9092",
            "logger": logger,
        })
        logger.info('Kafka producer connected!')

    @staticmethod
    def receipt(err, msg):
        if err:
            logger.error(f"Error: {err}")
        else:
            logger.info(f"Produced message on topic {msg.topic()} with value of {msg.value().decode('utf-8')}")

    def start_producer(self):
        self.create_producer()
        try:
            for event in EventSource(self.STREAMS_URL):
                self.producer.produce("wikimedia", event.data, callback=self.receipt)
                self.producer.flush()
        except KeyboardInterrupt:
            self.producer.flush()
            logger.info("Exited")
            exit(1)


if __name__ == "__main__":
    ProducerDemo().start_producer()
