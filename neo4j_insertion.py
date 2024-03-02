from py2neo import Graph

uri = "bolt://localhost:7687"
username = "neo4j"
password = "ARYA_2003"

graph = Graph(uri, auth=(username, password))

database_name = 'studentsdb'

uri_with_db = f"{uri}/db/{database_name}"
graph_db = Graph(uri_with_db, auth=(username, password))

csv_files_and_labels = [
    ('E:/datasets/250k.csv', 'd250k'),
    ('E:/datasets/500k.csv', 'd500k'),
    ('E:/datasets/750k.csv', 'd750k'),
    ('E:/datasets/1million.csv', 'd1million')
]

for csv_file_path, label in csv_files_and_labels:
    cypher_query = f"""
    LOAD CSV WITH HEADERS FROM 'file:///{csv_file_path}' AS row
    CREATE (:Student:{label} {{
        student_id: row.student_id,
        first_name: row.first_name,
        last_name: row.last_name,
        grade: row.grade,
        result: row.result,
        age: toInteger(row.age),
        gender: row.gender,
        presence_percentage: toFloat(row.presence_percentage)
    }})
    """

    graph_db.run(cypher_query)
    print(f"Data from {csv_file_path} inserted successfully.")

print("Data inserted into 'studentsdb' database.")
