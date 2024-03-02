import time
import redis
import os
import csv

redis_host = 'localhost'
redis_port = 6379
redis_db = 0

redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

if redis_client.ping():
    print("Connected to Redis")
else:
    print("Failed to connect to Redis")

datasets = [
    {'name': '250k', 'file_path': 'E:\\datasets\\250k.csv'},
    {'name': '500k', 'file_path': 'E:\\datasets\\500k.csv'},
    {'name': '750k', 'file_path': 'E:\\datasets\\750k.csv'},
    {'name': '1million', 'file_path': 'E:\\datasets\\1million.csv'}
]

base_directory = 'E:\\query_results\\redis'

os.makedirs(base_directory, exist_ok=True)

def sanitize_query_description(description):
    return ''.join(char if char.isalnum() else '_' for char in description)

for dataset in datasets:
    set_name = dataset['name']
    print(f"Dataset: {set_name}")

    total_time = 0
    execution_times = []

    for query_index, query_description in enumerate(["Q1", "Q2", "Q3", "Q4"]):
        sanitized_query_description = sanitize_query_description(query_description)

        for i in range(30):
            start_time = time.perf_counter_ns()

            if query_index == 0:
                all_students = redis_client.smembers(f'student_{set_name}')
            elif query_index == 1:
                name_prefix = 'A'
                students_with_name_prefix = [
                    student for student in redis_client.smembers(f'student_{set_name}')
                    if student.decode('utf-8').startswith(name_prefix)
                ]
            elif query_index == 2:
                gender = 'male'
                result = 'pass'
                students_with_gender_and_result = [
                    student for student in redis_client.smembers(f'student_{set_name}')
                    if redis_client.hget(f'student:{student.decode("utf-8")}_{set_name}', 'gender').decode('utf-8') == gender and
                    redis_client.hget(f'student:{student.decode("utf-8")}_{set_name}', 'result').decode('utf-8') == result
                ]
            elif query_index == 3:
                gender = 'female'
                min_presence_percentage = 80
                result = 'fail'
                students_with_criteria = [
                    student for student in redis_client.smembers(f'student_{set_name}')
                    if redis_client.hget(f'student:{student.decode("utf-8")}_{set_name}', 'gender').decode('utf-8') == gender and
                    int(redis_client.hget(f'student:{student.decode("utf-8")}_{set_name}', 'presence_percentage').decode('utf-8')) > min_presence_percentage and
                    redis_client.hget(f'student:{student.decode("utf-8")}_{set_name}', 'result').decode('utf-8') == result
                ]

            end_time = time.perf_counter_ns()
            execution_time = int(end_time - start_time)  #
            execution_times.append(execution_time)
            total_time += execution_time

            if i == 0:
                print(f"Query {query_index + 1}, First Execution Time: {execution_time} nanoseconds")
            if i == 29:
                avg_execution_time = total_time // 30
                print(f"Query {query_index + 1}, Average Execution Time: {avg_execution_time} nanoseconds")

    filename = os.path.join(base_directory, f"results_{set_name}.csv")
    with open(filename, 'w', newline='') as result_file:
        csv_writer = csv.writer(result_file)
        csv_writer.writerow(['Query', 'Execution Times'])
        for query_num in range(1, 5):
            query_label = f"Query {query_num}"
            response_times = execution_times[(query_num - 1) * 30: query_num * 30]
            csv_writer.writerow([query_label] + response_times)
