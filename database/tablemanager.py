class PostgresRepository:
    def __init__(self, connection):
        self.conn = connection

    def execute(self, query, params=None, fetch=False):
        with self.conn.cursor() as cur:
            cur.execute(query, params or ())
            if fetch:
                cols = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                return cols, rows
            else:
                self.conn.commit()
                return None