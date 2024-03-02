import redis
import csv
import json

redis_host = 'localhost'
redis_port = 6379
redis_db = 0

redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

if redis_client.ping():
    print("Connected to Redis")
else:
    print("Failed to connect to Redis")

csv_file_paths = ['E:\\datasets\\250k.csv', 'E:\\datasets\\500k.csv', 'E:\\datasets\\750k.csv',
                  'E:\\datasets\\1million.csv']

def import_csv_to_redis(file_path, redis_client):
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)

        dataset_name = file_path.split('\\')[-1].split('.')[0]

        dataset_data = []

        for row in reader:
            dataset_data.append(dict(row))

        redis_client.set(dataset_name, json.dumps(dataset_data))
        print(f"Data inserted successfully into Redis with key: {dataset_name}")

for file_path in csv_file_paths:
    import_csv_to_redis(file_path, redis_client)


'''
dataset_name = '250k'
dataset_data_json = redis_client.get(dataset_name)

if dataset_data_json:
    dataset_data = json.loads(dataset_data_json)
    print(f"Data for dataset '{dataset_name}':")
    for row in dataset_data:
        print(row)
else:
    print(f"Dataset '{dataset_name}' not found in Redis.")
'''



















