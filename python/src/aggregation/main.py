import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top = {}
        self.eof_count = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data for client %s: %s", client_id, fruit)
        if client_id not in self.fruit_top:
            self.fruit_top[client_id] = []
        top = self.fruit_top[client_id]
        for i in range(len(top)):
            if top[i].fruit == fruit:
                new_item = top[i] + fruit_item.FruitItem(fruit, amount)
                del top[i]
                bisect.insort(top, new_item)
                return
        bisect.insort(top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        logging.info("Received EOF for client %s", client_id)
        self.eof_count[client_id] = self.eof_count.get(client_id, 0) + 1
        if self.eof_count[client_id] < SUM_AMOUNT:
            return

        top = self.fruit_top.pop(client_id, [])
        del self.eof_count[client_id]

        fruit_chunk = list(top[-TOP_SIZE:])
        fruit_chunk.reverse()
        # mapeampos para mandar al serialize
        fruit_top_list = list(
            map(lambda fi: [fi.fruit, fi.amount], fruit_chunk)
        )
        logging.info("Sending top for client %s: %s", client_id, fruit_top_list)
        self.output_queue.send(
            message_protocol.internal.serialize([client_id, fruit_top_list])
        )

    def process_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        elif len(fields) == 1: # eof
            self._process_eof(fields[0])
        ack()

    def stop(self):
        self.input_exchange.stop_consuming()

    def close(self):
        self.input_exchange.close()
        self.output_queue.close()

    def start(self):
        self.input_exchange.start_consuming(self.process_message)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()

    def handle_sigterm(_signum, _frame):
        logging.info("Received SIGTERM signal")
        aggregation_filter.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    aggregation_filter.start()
    aggregation_filter.close()
    return 0


if __name__ == "__main__":
    main()
