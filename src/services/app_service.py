from services.utils import get_db_connection

class AppService:
    def __init__(self):
        self.conn = get_db_connection()
        self.cursor = self.conn.cursor()
    
    def get_app_id(self) -> int:
        app_name = 'worker-dispatcher'
        self.cursor = self.conn.cursor()

        self.cursor.execute("""
            SELECT id FROM cbx.applications WHERE name = %s
        """, (app_name,))

        result = self.cursor.fetchone()
        if result:
            application_id = result[0]
            return application_id
        else:
            raise ValueError(f"Aplicação '{app_name}' não encontrada.")