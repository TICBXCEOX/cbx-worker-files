import logging

class DynamicLoggerAdapter(logging.LoggerAdapter):
    def __init__(self, logger, extra=None):
        super().__init__(logger, extra or {})
        self._transaction_id = None

    def set_transaction_id(self, tx_id):
        self._transaction_id = tx_id

    def clear_transaction_id(self):
        self._transaction_id = None

    def process(self, msg, kwargs):
        # Injeta o transaction_id no extra (como campo, não na mensagem)
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        kwargs['extra']['transaction_id'] = self._transaction_id or None
        return msg, kwargs