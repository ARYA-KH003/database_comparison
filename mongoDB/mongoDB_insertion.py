from pymongo import MongoClient
import pandas as pd

mongo_client = MongoClient()
mongo_db = mongo_client['students_db']

csv_files = ['E:\\datasets\\1million.csv', 'E:\\datasets\\750k.csv', 'E:\\datasets\\500k.csv', 'E:\\datasets\\250k.csv']

def insert_data_mongodb(data, collection):

    collection.insert_many(data)


for csv_file in csv_files:

    with open(csv_file, 'r') as file:
        data = pd.read_csv(file)

    collection_name = csv_file.split('\\')[-1].split('.')[0]  # Extract the collection name from the file path
    insert_data_mongodb(data.to_dict('records'), mongo_db[collection_name])
    print(f"Data inserted successfully into collection: {collection_name}")

mongo_client.close()