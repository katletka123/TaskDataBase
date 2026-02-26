import psycopg2
from config import Config

class DatabaseConnection:
    def __enter__(self):
        self.conn = psycopg2.connect(**Config.DB_CONFIG)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()