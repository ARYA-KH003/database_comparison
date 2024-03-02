from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, ConsistencyLevel
from cassandra import ReadFailure
import time
import os
import csv

base_directory = r'E:\query_results\Cassandra'

tables = ['table_250k', 'table_500k', 'table_750k', 'table_1million']

queries = [
    "SELECT * FROM {} WHERE gender = 'Female' AND presence_percentage > 80 AND result = 'Fail' ALLOW FILTERING;",
    "SELECT * FROM {} WHERE gender='Male' and result='Pass' ALLOW FILTERING;",
    "SELECT * FROM {} WHERE first_name LIKE 'A%' ALLOW FILTERING;",
    "SELECT * FROM {};"
]

max_retries = 3
delay_seconds = 1

query_execution_times = {f"Query {i + 1} execution times": [] for i in range(len(queries))}

cluster = Cluster(['localhost'])
session = cluster.connect('students')

for table in tables:
    dataset_results = {f"Query {i + 1}": [] for i in range(len(queries))}

    for i, query_template in enumerate(queries):
        execution_times = []

        query = query_template.format(table)
        statement = SimpleStatement(query, consistency_level=ConsistencyLevel.QUORUM)

        for j in range(30):
            retries = 0
            total_execution_time = 0

            while retries < max_retries:
                try:
                    start_time = time.time()
                    result = session.execute(statement)
                    for row in result:
                        pass
                    end_time = time.time()
                    execution_time = int((end_time - start_time) * 1e9)
                    execution_times.append(execution_time)
                    total_execution_time += execution_time
                    break
                except ReadFailure as e:
                    print(f"ReadFailure on execution {j + 1} of Query {i + 1}: {e}")
                    retries += 1
                    if retries < max_retries:
                        delay = delay_seconds * (2 ** (retries - 1))
                        print(f"Retrying in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        print(f"Failed after retries: {e}")

            if j == 0:
                print(f"Table: {table}, Query {i + 1}, First Execution Time: {execution_time} nanoseconds")
            if j == 29:
                avg_execution_time = total_execution_time // (retries + 1)
                print(f"Table: {table}, Query {i + 1}, Average Execution Time: {avg_execution_time} nanoseconds")

        query_execution_times[f"Query {i + 1} execution times"].append(execution_times)
        dataset_results[f"Query {i + 1}"] = execution_times

    filename = os.path.join(base_directory, f"results_{table.replace('table_', '')}.csv")
    with open(filename, 'w', newline='') as result_file:
        csv_writer = csv.writer(result_file)
        csv_writer.writerow(['Query', 'Execution Times'])
        for query_num in range(1, 5):
            query_label = f"Query {query_num}"
            response_times = dataset_results[query_label]
            csv_writer.writerow([query_label] + response_times)

session.shutdown()
cluster.shutdown()
