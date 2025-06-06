import logging
from datetime import datetime

from services.app_service import AppService
from services.utils import get_db_connection

class PostgresLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.app_service = AppService()
        self.application_id = self.app_service.get_app_id()
        self.conn = get_db_connection()
        self.cursor = self.conn.cursor()
        
    def emit(self, record):
        try:
            timestamp = datetime.now()
            level = record.levelname
            message = record.getMessage()
            transaction_id = getattr(record, "transaction_id", None)
            context = getattr(record, "context", None)

            self.cursor.execute("""
                INSERT INTO cbx.applications_logs (application_id, timestamp, level, message, transaction_id, context)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (self.application_id, timestamp, level, message, transaction_id, context))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            self.handleError(record)

    def close(self):
        self.cursor.close()
        self.conn.close()
        super().close()
