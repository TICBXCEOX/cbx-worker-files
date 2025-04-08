from services.utils import get_db_connection

def get_ncms():
    with get_db_connection() as connection, connection.cursor() as cursor:
        query = """
            SELECT id, ncm, status, type_ncm, group_ncm, properties
            FROM cbx.ncms
        """
        cursor.execute(query)
        rs = cursor.fetchall()
        results = [
            {
                "id": ncms.id,
                "ncm_number": ncms.ncm,
                "staus": ncms.status,
                "type": ncms.type_ncm,
                "group": ncms.group_ncm,
                "properties": ncms.properties
            } for ncms in rs]

        return rs, results
