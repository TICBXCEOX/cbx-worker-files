import logging

from logger.logger_adapter import DynamicLoggerAdapter
from logger.logger_postgre import PostgresLogHandler

class LoggerService:
    def __init__(self):
        self.logger = self.setup_log()

    def setup_log(self) -> DynamicLoggerAdapter:
        logger = logging.getLogger(__name__)
        
        if getattr(logger, "_custom_handlers_added", False):
            return DynamicLoggerAdapter(logger)                

        logger.setLevel(logging.INFO)

        # Formatter para logs
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Handler para console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)        
        logger.addHandler(console_handler)

        # PostgreSQL handler        
        pg_handler = PostgresLogHandler()
        pg_handler.setFormatter(formatter)
        logger.addHandler(pg_handler)
        
        logger._custom_handlers_added = True
                
        return DynamicLoggerAdapter(logger)
    
    def set_transaction_id(self, tx_id):
        self.logger.set_transaction_id(tx_id)

    def clear_transaction_id(self):
        self.logger.clear_transaction_id()

    def warn(self, msg):
        self.logger.warning(msg)
    
    def info(self, msg):
        self.logger.info(msg)
    
    def error(self, msg):
        self.logger.error(msg)
