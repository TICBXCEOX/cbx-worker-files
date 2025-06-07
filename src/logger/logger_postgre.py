import logging

from datetime import datetime
from services.app_service import AppService
from services.utils import get_pool

class PostgresLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.app_service = AppService()
        self.pool = get_pool()
        self.application_id = self.app_service.get_app_id()
        
    def emit(self, record):
        conn = None
        cursor = None
        try:
            conn = self.pool.getconn()
            cursor = conn.cursor()

            timestamp = datetime.now()
            level = record.levelname
            message = record.getMessage()
            transaction_id = getattr(record, "transaction_id", None)
            context = getattr(record, "context", None)

            cursor.execute("""
                INSERT INTO cbx.applications_logs (application_id, timestamp, level, message, transaction_id, context)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (self.application_id, timestamp, level, message, transaction_id, context))
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            self.handleError(record)
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.pool.putconn(conn)            

    def close(self):
        super().close()
