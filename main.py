import json
import psycopg2

from database.studentmanager import StudentsTable
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



        students_con=StudentsTable(conn)
        students_con.create_students_table_query()
        students_con.insert_query(data_students)

        results = [
            ("result_min_room_students_query", students_con.min_age_rooms_query()),
            ("result_max_age_difference_rooms_query", students_con.max_diference_age_query()),
            ("result_diference_sex_rooms_query", students_con.diference_sex_rooms_query())
        ]

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


