from dataclasses import dataclass
from database.tablemanager import PostgresRepository

@dataclass
class Student:
    id: int
    name: str
    birthday: str
    room: int
    sex: str
class StudentsTable(PostgresRepository):
    def  create_students_table_query(self):
        query="""
        CREATE TABLE IF NOT EXISTS students (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            birthday TIMESTAMP NOT NULL,
            room INT REFERENCES rooms(id),
            sex CHAR(1) CHECK (sex IN ('M', 'F'))
        );"""
        self.execute_command(query)
        self.execute_command("""CREATE INDEX IF NOT EXISTS index_students_room
        ON students(room);
        """)

    def insert_query(self,data_students):
        query="""
        INSERT INTO students (id, name, birthday, room, sex)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """
        for student in data_students:
            self.execute_command(
                query,(
                    student["id"],
                    student["name"],
                    student["birthday"],
                    student["room"],
                    student["sex"]
                )
            )

    def room_list_query(self):
        query="""
        SELECT
            rooms.id,
            COUNT(students.name) AS students_count
        FROM rooms
        INNER JOIN students ON rooms.id = students.room
        GROUP BY rooms.id;
        """
        return self.select_command(query)

    def  min_age_rooms_query(self):
        query="""
        SELECT
            rooms.name,
            CAST(AVG(EXTRACT(YEAR FROM age(students.birthday))) AS INTEGER) AS avg_age
            
        FROM students
        INNER JOIN rooms ON students.room=rooms.id
        GROUP BY rooms.name
        ORDER BY avg_age
        LIMIT 5;
        """
        return self.select_command(query)

    def  max_diference_age_query(self):
        query="""
        SELECT
            rooms.name,
            CAST(MAX(EXTRACT(YEAR FROM age(students.birthday)))
               - MIN(EXTRACT(YEAR FROM age(students.birthday)))AS INTEGER) AS diference_age
           
        FROM students
        INNER JOIN rooms ON students.room=rooms.id
        GROUP BY rooms.name
        ORDER BY diference_age DESC
        LIMIT 5;
        """
        return self.select_command(query)

    def diference_sex_rooms_query(self):
        query="""
        SELECT rooms.name
        FROM students
        INNER JOIN rooms ON students.room=rooms.id
        GROUP BY rooms.name
        HAVING COUNT(DISTINCT sex) > 1;
        """
        return self.select_command(query)