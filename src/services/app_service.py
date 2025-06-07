from services.utils import get_db_connection, get_pool

class AppService:
    def __init__(self):
        self.pool = get_pool()
    
    def get_app_id(self) -> int:
        app_name = 'worker-dispatcher'
        conn = None
        cursor = None
        try:
            conn = self.pool.getconn()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT id FROM cbx.applications WHERE name = %s
            """, (app_name,))

            result = cursor.fetchone()
            if result:
                application_id = result[0]
                return application_id
            else:
                raise ValueError(f"Aplicação '{app_name}' não encontrada.")
        except Exception:
            if conn:
                conn.rollback()
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.pool.putconn(conn)            