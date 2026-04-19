from common import message_protocol


class MessageHandler:
    _next_client_id = 0

    def __init__(self):
        self.client_id = MessageHandler._next_client_id
        MessageHandler._next_client_id += 1

    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.client_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_id])

    # deserializa solo los resultados que correspondan al cliente que esta entablando la comunicacion
    # el client_id esta en fields[0]
    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if fields[0] != self.client_id:
            return None
        return fields[1]
