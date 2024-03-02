from py2neo import Graph

uri = "bolt://localhost:7687"
username = "neo4j"
password = "ARYA_2003"

graph = Graph(uri, auth=(username, password))

database_name = 'studentsdb'

uri_with_db = f"{uri}/db/{database_name}"
graph_db = Graph(uri_with_db, auth=(username, password))

cypher_query = """
MATCH (s:Student:d1million)
RETURN s.student_id, s.first_name, s.last_name, s.grade, s.result, s.age, s.gender, s.presence_percentage
"""

result = graph_db.run(cypher_query)

for record in result:
    print(record)
