class PostgresRepository:
    def __init__(self, connection):
        self.conn = connection

    # не селект
    def execute_command(self, query, params=None):
        with self.conn.cursor() as cur:
            cur.execute(query, params)
        self.conn.commit()

    # сеоект
    def select_command(self, query, params=None):
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
        return columns, rows
