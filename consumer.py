import logging
from confluent_kafka import Consumer
from opensearchpy import OpenSearch
import opensearchpy
import json


logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ConsumerDemo:
    INDEX_NAME = "wikimedia-changes"

    def init_opensearch_client(self):
        open_search_client = OpenSearch(
            hosts=[{"host": "localhost", "port": "9200"}],
            http_compress=True,
            use_ssl=False,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
        )
        logger.info("Creating index:")
        try:
            response = open_search_client.indices.create(self.INDEX_NAME)
            logger.info(response)
        except opensearchpy.exceptions.RequestError:
            logger.info("Index already exists")
        return open_search_client

    def start_consumer(self):
        consumer = Consumer({"bootstrap.servers": "localhost:9092", "group.id": 1})
        consumer.subscribe(["wikimedia"])
        opensearch_client = self.init_opensearch_client()

        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Error: {msg.error()}")
                    continue

                try:
                    data = json.loads(msg.value().decode("utf-8"))
                    response = opensearch_client.index(
                        index=self.INDEX_NAME,
                        body=data,
                        id=data["meta"]["id"],
                        refresh=True,
                    )
                    logger.info(f"Inserted doc. Response id {response['_id']}")
                except Exception:
                    pass
        except KeyboardInterrupt:
            consumer.close()
            logger.info("Exited")
            exit(1)


if __name__ == "__main__":
    ConsumerDemo().start_consumer()
