import json
import psycopg2

conn = psycopg2.connect(
    dbname="mydatabase",
    user="myuser",
    password="mypassword",
    host="localhost",
    port="5433"
)
cur = conn.cursor()



#room table
with open("rooms.json", "r", encoding="utf-8") as f:
    data_rooms = json.load(f)

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



#student table
with open("students.json", "r", encoding="utf-8") as f:
    data = json.load(f)

create_table_query="""
CREATE TABLE IF NOT EXISTS students (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    birthday TIMESTAMP NOT NULL,
    room INT REFERENCES rooms(id),
    sex CHAR(1) CHECK (sex IN ('M', 'F'))
);

"""
cur.execute(create_table_query)

insert_query = """
INSERT INTO students (id, name, birthday, room, sex)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (id) DO NOTHING;
"""

for student in data:
    cur.execute(insert_query, (
        student["id"],
        student["name"],
        student["birthday"],
        student["room"],
        student["sex"]
    ))

select_query= """
SELECT
    rooms.id,
    COUNT(students.name) AS students_count
FROM rooms
LEFT JOIN students ON rooms.id = students.room
GROUP BY rooms.id;
"""
cur.execute(select_query)

results = cur.fetchall()
for row in results:
    print(row)

conn.commit()
cur.close()
conn.close()

