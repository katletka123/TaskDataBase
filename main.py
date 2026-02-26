import json
import psycopg2
from exporters.exporter_mapping import exporter_mapping
import argparse
from database.connection import DatabaseConnection
from reader.json_reader import JsonReader

parser=argparse.ArgumentParser()
parser.add_argument("--format", default="json", help="Format of file")
parser.add_argument("--students_path", default="students.json", help="Path to students file")
parser.add_argument("--room_path", default="rooms.json", help="Path to room file")

args=parser.parse_args()
export_format=args.format
students_file=args.students_path
room_file=args.room_path


exporter=exporter_mapping[export_format]


with DatabaseConnection() as conn:
    with conn.cursor() as cur:

        reader=JsonReader()
        data_rooms=reader.read_json(room_file)
        data_students=reader.read_json((students_file))


        create_rooms_table_query="""
        CREATE TABLE IF NOT EXISTS rooms (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL
            );
        
        """
        cur.execute(create_rooms_table_query)

        insert_rooms_query="""
        INSERT INTO rooms (id, name)
        VALUES (%s,%s) 
        ON CONFLICT (id) DO NOTHING;
        """
        for room in data_rooms:
            cur.execute(insert_rooms_query, (
                room["id"],
                room["name"]
            ))




        create_students_table_query="""
        CREATE TABLE IF NOT EXISTS students (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            birthday TIMESTAMP NOT NULL,
            room INT REFERENCES rooms(id),
            sex CHAR(1) CHECK (sex IN ('M', 'F'))
        );
            
        CREATE INDEX IF NOT EXISTS index_students_room
        ON students(room);
        """
        cur.execute(create_students_table_query)

        insert_query = """
        INSERT INTO students (id, name, birthday, room, sex)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """

        for student in data_students:
            cur.execute(insert_query, (
                student["id"],
                student["name"],
                student["birthday"],
                student["room"],
                student["sex"]
            ))

        room_list_query= """
        SELECT
            rooms.id,
            COUNT(students.name) AS students_count
        FROM rooms
        INNER JOIN students ON rooms.id = students.room
        GROUP BY rooms.id;
        """


        min_age_rooms_query="""
        SELECT
            rooms.name,
            CAST(AVG(EXTRACT(YEAR FROM age(students.birthday))) AS INTEGER) AS avg_age
            
        FROM students
        INNER JOIN rooms ON students.room=rooms.id
        GROUP BY rooms.name
        ORDER BY avg_age
        LIMIT 5;
        """

        max_diference_age_query="""
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

        diference_sex_rooms_query="""
        SELECT rooms.name
        FROM students
        INNER JOIN rooms ON students.room=rooms.id
        GROUP BY rooms.name
        HAVING COUNT(DISTINCT sex) > 1;
        """


        cur.execute(room_list_query)
        column_names = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        exporter.export(column_names, rows,"result_room_students_count")


        cur.execute(min_age_rooms_query)
        column_names = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        exporter.export(column_names,rows, "result_min_room_students_query")


        cur.execute(max_diference_age_query)
        column_names = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        exporter.export(column_names,rows, "result_max_age_diference_rooms_query")


        cur.execute(diference_sex_rooms_query)
        column_names = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        exporter.export(column_names,rows, "result_diference_sex_rooms_query")


    conn.commit()


