from dataclasses import dataclass

from database.tablemanager import PostgresRepository


@dataclass
class Room:
    id: int
    name: str


class RoomTable(PostgresRepository):
    def create_rooms_table_query(self):
        query = """
        CREATE TABLE IF NOT EXISTS rooms (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL
            );
        
        """
        self.execute_command(query)

    def insert_rooms_query(self, data_rooms):
        query = """
                INSERT INTO rooms (id, name)
                VALUES (%s, %s) ON CONFLICT (id) DO NOTHING; \
                """
        for room in data_rooms:
            self.execute_command(query, (room["id"], room["name"]))
