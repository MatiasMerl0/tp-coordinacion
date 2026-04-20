import pika

from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)


def _is_connection_lost(exception):
    return isinstance(exception, (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError))


# helper method para handlear errores
def _raise_middleware_error(e, default_error=MessageMiddlewareMessageError):
    if _is_connection_lost(e):
        raise MessageMiddlewareDisconnectedError from e
    raise default_error from e


class _RabbitMQBase:
    def __init__(self, host):
        self._connection = None
        self._channel = None
        self._consuming = False
        self._queue_name = None
        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )
            self._channel = self._connection.channel()
        except Exception as e:
            _raise_middleware_error(e)

    # metodo que vamos a inyectar en el callback
    def _ack(self, delivery_tag):
        self._channel.basic_ack(delivery_tag=delivery_tag)

    # metodo que vamos a inyectar en el callback
    def _nack(self, delivery_tag):
        self._channel.basic_nack(delivery_tag=delivery_tag, requeue=False)

    def start_consuming(self, on_message_callback):
        def _on_message(_ch, method, _properties, body):
            on_message_callback(
                body,
                lambda: self._ack(method.delivery_tag),
                lambda: self._nack(method.delivery_tag),
            )

        try:
            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=_on_message,
                auto_ack=False,
            )
            self._consuming = True
            self._channel.start_consuming()
        except Exception as e:
            _raise_middleware_error(e)
        finally: # nos aseguramos de que siempre se marque como dejando de consumir
            self._consuming = False

    def stop_consuming(self):
        if not self._consuming:
            return
        try:
            # detiene la escucha. El finally de start_consuming setea en false el flag de consuming
            self._channel.stop_consuming()
        except Exception as e:
            _raise_middleware_error(e)

    def close(self):
        try:
            if self._consuming and self._channel.is_open:
                self._channel.stop_consuming()
            self._consuming = False
            # si llamamos close 2 veces, connection se setea a None, por eso el safety check
            if self._connection is not None and self._connection.is_open:
                self._connection.close()
        except Exception as e:
            _raise_middleware_error(e, MessageMiddlewareCloseError)
        finally:
            self._connection = None
            self._channel = None


class MessageMiddlewareQueueRabbitMQ(_RabbitMQBase, MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        super().__init__(host)
        self._queue_name = queue_name
        try:
            self._channel.queue_declare(queue=queue_name, durable=False)
        except Exception as e:
            _raise_middleware_error(e)

    def send(self, message):
        try:
            self._channel.basic_publish(
                exchange="",
                routing_key=self._queue_name,
                body=message,
            )
        except Exception as e:
            _raise_middleware_error(e)


class MessageMiddlewareExchangeRabbitMQ(_RabbitMQBase, MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        super().__init__(host)
        self._exchange_name = exchange_name
        self._routing_keys = routing_keys
        try:
            self._channel.exchange_declare(exchange=exchange_name, exchange_type="direct")
        except Exception as e:
            _raise_middleware_error(e)

    def send(self, message):
        try:
            for routing_key in self._routing_keys:
                self._channel.basic_publish(
                    exchange=self._exchange_name,
                    routing_key=routing_key,
                    body=message,
                )
        except Exception as e:
            _raise_middleware_error(e)

    # crea cola anonima al empezar a consumir para que no la creen los productores
    # ya que recibirían sus propios mensajes.
    def start_consuming(self, on_message_callback):
        try:
            # evitamos crear la cola mas de una vez si se llama varias veces el metodo
            if self._queue_name is None:
                result = self._channel.queue_declare(queue="", exclusive=True)
                self._queue_name = result.method.queue
            for routing_key in self._routing_keys:
                self._channel.queue_bind(
                    exchange=self._exchange_name,
                    queue=self._queue_name,
                    routing_key=routing_key,
                )
        except Exception as e:
            _raise_middleware_error(e)
        super().start_consuming(on_message_callback)
