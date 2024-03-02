from py2neo import Graph, DatabaseError
import time
import os
import csv

uri = "bolt://localhost:7687"
username = "neo4j"
password = "ARYA_2003"

graph = Graph(uri, auth=(username, password))

labels = ['d250k', 'd500k', 'd750k', 'd1million']
datasets = ['250k', '500k', '750k', '1million']

base_directory = 'E:\\query_results\\neo4j'

os.makedirs(base_directory, exist_ok=True)

def sanitize_query_description(description):
    return ''.join(char if char.isalnum() else '_' for char in description)

queries = [
    "MATCH (s:Student:{}) WHERE s.gender = 'Female' AND s.presence_percentage > 80 AND s.result = 'Fail' RETURN s",
    "MATCH (s:Student:{}) WHERE s.gender = 'Male' AND s.result = 'Pass' RETURN s",
    "MATCH (s:Student:{}) WHERE s.first_name STARTS WITH 'A' RETURN s",
    "MATCH (s:Student:{}) RETURN s"
]

for label, dataset in zip(labels, datasets):
    print(f"Queries for label '{label}':")

    dataset_results = {f"Query {i}": [] for i in range(1, 5)}

    for query_idx, query_template in enumerate(queries, start=1):
        total_time = 0
        execution_times = []

        for i in range(1, 31):
            try:
                start_time = time.perf_counter_ns()
                result = graph.run(query_template.format(label))
                for record in result:
                    pass
                end_time = time.perf_counter_ns()
                execution_time = int(end_time - start_time)
                execution_times.append(execution_time)
                total_time += execution_time

                if i == 1:
                    print(f"Label: {label}, Query {query_idx}, First Execution Time: {execution_time} nanoseconds")
                if i == 30:
                    avg_execution_time = total_time // 30
                    print(f"Label: {label}, Query {query_idx}, Average Execution Time: {avg_execution_time} nanoseconds.")

            except DatabaseError as e:
                print(f"Error executing query: {e}")
                time.sleep(1)
                continue

        dataset_results[f"Query {query_idx}"] = execution_times

    filename = os.path.join(base_directory, f"results_{dataset}.csv")
    with open(filename, 'w', newline='') as result_file:
        csv_writer = csv.writer(result_file)
        csv_writer.writerow(['Query', 'Execution Times'])
        for query_num in range(1, 5):
            query_label = f"Query {query_num}"
            response_times = dataset_results[query_label]
            csv_writer.writerow([query_label] + response_times)
