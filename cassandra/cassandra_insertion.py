from cassandra.cluster import Cluster
import pandas as pd

cassandra_host = 'localhost'

cluster = Cluster([cassandra_host])
session = cluster.connect()

keyspace_name = 'students'
replication_strategy = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
}

create_keyspace_query = f"""
    CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
    WITH REPLICATION = {str(replication_strategy)};
"""
session.execute(create_keyspace_query)

session.set_keyspace(keyspace_name)

tables = [
    {'table_name': 'table_250k', 'csv_file_path': 'E:\\datasets\\250k.csv'},
    {'table_name': 'table_500k', 'csv_file_path': 'E:\\datasets\\500k.csv'},
    {'table_name': 'table_750k', 'csv_file_path': 'E:\\datasets\\750k.csv'},
    {'table_name': 'table_1million', 'csv_file_path': 'E:\\datasets\\1million.csv'}
]

for table in tables:
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table['table_name']} (
            student_id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            grade DOUBLE,
            result TEXT,
            age INT,
            gender TEXT,
            presence_percentage DOUBLE
        );
    """
    session.execute(create_table_query)

for table in tables:
    csv_file_path = table['csv_file_path']
    table_name = table['table_name']

    df = pd.read_csv(csv_file_path)
    count = 0

    for _, row in df.iterrows():
        insert_query = f"""
            INSERT INTO {table_name} (student_id, first_name, last_name, grade, result, age, gender, presence_percentage)
            VALUES (UUID(), '{row['first_name']}', '{row['last_name']}', {row['grade']}, '{row['result']}', {row['age']}, '{row['gender']}', {row['presence_percentage']});
        """
        session.execute(insert_query)
        count += 1
        print(f"{count}Data from {table['csv_file_path']} has been inserted.")
        print("------------------")

session.shutdown()
cluster.shutdown()
