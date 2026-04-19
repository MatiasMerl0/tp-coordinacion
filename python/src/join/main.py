import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.partial_tops = {}
        self.received_count = {}

    def process_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        client_id = fields[0]
        partial_top = fields[1]
        logging.info("Received partial top for client %s", client_id)

        if client_id not in self.partial_tops:
            self.partial_tops[client_id] = []
            self.received_count[client_id] = 0

        self.received_count[client_id] += 1
        self.partial_tops[client_id].extend(partial_top)

        if self.received_count[client_id] == AGGREGATION_AMOUNT:
            all_items = [
                fruit_item.FruitItem(f, a)
                for f, a in self.partial_tops[client_id]
            ]
            all_items.sort()
            all_items.reverse()
            final_top = [
                [fi.fruit, fi.amount] for fi in all_items[:TOP_SIZE]
            ]

            logging.info("Sending final top for client %s: %s", client_id, final_top)
            self.output_queue.send(
                message_protocol.internal.serialize([client_id, final_top])
            )

            del self.partial_tops[client_id]
            del self.received_count[client_id]

        ack()

    def stop(self):
        self.input_queue.stop_consuming()

    def close(self):
        self.input_queue.close()
        self.output_queue.close()

    def start(self):
        self.input_queue.start_consuming(self.process_message)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()

    def handle_sigterm(_signum, _frame):
        logging.info("Received SIGTERM signal")
        join_filter.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    join_filter.start()
    join_filter.close()
    return 0


if __name__ == "__main__":
    main()
