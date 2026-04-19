import os
import logging
import signal
import hashlib
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

# devuelve un numero deterministico a partir de nombre de la fruta
def _fruit_hash(fruit):
    # md5 solo acepta bytes asi que encodeamos antes
    return int(hashlib.md5(fruit.encode()).hexdigest(), 16)

class SumFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        # exchange para broadcast de EOF entre Sums (productor, usado por el main thread)
        # todos los Sums se bindean a la misma routing key -> funciona como fanout
        eof_exchange_name = f"{SUM_PREFIX}_eof"
        self.eof_broadcast_producer = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, eof_exchange_name, ["eof"]
        )
        self.amount_by_fruit = {}
        self.lock = threading.Lock()


    # Thread secundario que escucha broadcasts de EOF y flushea los datos acumulados.
    def _eof_listener(self):
        eof_exchange_name = f"{SUM_PREFIX}_eof"
        eof_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, eof_exchange_name, ["eof"]
        )
        output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            output_exchanges.append(exchange)

        def on_eof(message, ack, nack):
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]
            logging.info("Received EOF broadcast for client %s, flushing", client_id)
            # el lock garantiza que esperamos a que el main thread termine
            # de procesar su mensaje actual antes de hacer el flush
            with self.lock:
                client_data = self.amount_by_fruit.pop(client_id, {})
            # mandamos las sumas locales a cada aggregator (fuera del lock)
            for fi in client_data.values():
                target_agg = _fruit_hash(fi.fruit) % AGGREGATION_AMOUNT
                output_exchanges[target_agg].send(
                    message_protocol.internal.serialize(
                        [client_id, fi.fruit, fi.amount]
                    )
                )
            # mandamos el EOF como un array con solo el client_id a cada aggregator
            for exchange in output_exchanges:
                exchange.send(message_protocol.internal.serialize([client_id]))
            ack()

        eof_consumer.start_consuming(on_eof)

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data for client %s: %s", client_id, fruit)
        with self.lock:
            if client_id not in self.amount_by_fruit:
                self.amount_by_fruit[client_id] = {}
            client_data = self.amount_by_fruit[client_id]
            client_data[fruit] = client_data.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

    def process_data_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3: # [client_id, fruit, amount]
            self._process_data(*fields)
        elif len(fields) == 1: # [client_id] es el EOF
            client_id = fields[0]
            logging.info("Broadcasting EOF for client %s", client_id)
            self.eof_broadcast_producer.send(
                message_protocol.internal.serialize([client_id])
            )
        ack()

    def stop(self):
        self.input_queue.stop_consuming()

    def close(self):
        self.input_queue.close()
        self.eof_broadcast_producer.close()

    def start(self):
        control_thread = threading.Thread(
            target=self._eof_listener, daemon=True
        )
        control_thread.start()
        self.input_queue.start_consuming(self.process_data_message)


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()

    def handle_sigterm(_signum, _frame):
        logging.info("Received SIGTERM signal")
        sum_filter.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    sum_filter.start()
    sum_filter.close()
    return 0


if __name__ == "__main__":
    main()
