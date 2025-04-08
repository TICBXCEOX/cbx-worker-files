import logging

class LoggerService:
    def __init__(self):
        self.logger = self.setup_log()

    def setup_log(self):        
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        # Formatter para logs
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Handler para console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        if not logger.hasHandlers():
            logger.addHandler(console_handler)

        return logger
    
    def warn(self, msg):
        self.logger.warning(msg)
    
    def info(self, msg):
        self.logger.info(msg)
    
    def error(self, msg):
        self.logger.error(msg)
