import os
import csv
from faker import Faker
import random

fake = Faker()

fields = ['student_id', 'first_name', 'last_name', 'grade', 'result', 'age', 'gender', 'presence_percentage']

csv_file_path = 'E:\\datasets\\1million.csv'
if not os.path.exists(csv_file_path):
    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(fields)

def generate_student_data(student_id):
    first_name = fake.first_name()
    last_name = fake.last_name()
    grade = fake.random_int(min=0, max=30)
    result = 'Pass' if grade > 18 else 'Fail'
    age = fake.random_int(min=18, max=25)
    gender = fake.random_element(elements=['Male', 'Female'])
    presence_percentage = round(random.uniform(70, 100), 2)
    return [student_id, first_name, last_name, grade, result, age, gender, presence_percentage]

num_records_1million = 1000000

with open(csv_file_path, 'w', newline='') as csvfile_1million:
    writer_1million = csv.writer(csvfile_1million)
    writer_1million.writerow(fields)
    for student_id in range(1, num_records_1million + 1):
        data = generate_student_data(student_id)
        writer_1million.writerow(data)

csv_data = []
with open(csv_file_path, 'r') as csvfile_1million:
    reader = csv.reader(csvfile_1million)
    next(reader)
    csv_data = list(reader)

data_750k = csv_data[:750000]
data_500k = csv_data[:500000]
data_250k = csv_data[:250000]

csv_file_750k = 'E:\\datasets\\750k.csv'
csv_file_500k = 'E:\\datasets\\500k.csv'
csv_file_250k = 'E:\\datasets\\250k.csv'

def write_csv_data(file_name, data):
    with open(file_name, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(fields)
        writer.writerows(data)

write_csv_data(csv_file_750k, data_750k)
write_csv_data(csv_file_500k, data_500k)
write_csv_data(csv_file_250k, data_250k)


print("fake data generation and splitting is done.")
