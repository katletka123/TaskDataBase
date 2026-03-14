from database.studentmanager import StudentsTable
from exporters.exporter_mapping import exporter_mapping
import argparse
from database.connection import DatabaseConnection
from reader.json_reader import JsonReader

parser = argparse.ArgumentParser()
parser.add_argument("--format", default="json", help="Format of file")
parser.add_argument(
    "--students_path", default="students.json", help="Path to students file"
)
parser.add_argument("--room_path", default="rooms.json", help="Path to room file")

args = parser.parse_args()
export_format = args.format
students_file = args.students_path
room_file = args.room_path


exporter = exporter_mapping[export_format]


with DatabaseConnection() as conn:

    reader = JsonReader()
    data_rooms = reader.read_json(room_file)
    data_students = reader.read_json(students_file)

    students_con = StudentsTable(conn)

    students_con.create_students_table_query()

    students_con.insert_query(data_students)

    results = [
        ("room_list_query", students_con.room_list_query()),
        ("result_min_room_students_query", students_con.min_age_rooms_query()),
        (
            "result_max_age_difference_rooms_query",
            students_con.max_diference_age_query(),
        ),
        ("result_diference_sex_rooms_query", students_con.diference_sex_rooms_query()),
    ]

    for file_name, (columns, rows) in results:
        exporter.export(columns, rows, file_name)
