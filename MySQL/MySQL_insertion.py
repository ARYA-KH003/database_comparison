import mysql.connector
import pandas as pd
from sqlalchemy import create_engine

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="ARYA_2003",
    port="3305",
    database="students_db",
    connect_timeout=120
)

cursor = connection.cursor()

database_name = "students_db"
check_database_query = f"SHOW DATABASES LIKE '{database_name}';"
cursor.execute(check_database_query)

database_exists = False
for (database,) in cursor:
    if database == database_name:
        database_exists = True
        break

if not database_exists:
    create_database_query = f"CREATE DATABASE {database_name};"
    cursor.execute(create_database_query)

use_database_query = f"USE {database_name};"
cursor.execute(use_database_query)

create_table_query = """
CREATE TABLE IF NOT EXISTS 1million ( 
    student_id INT PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    grade INT,
    result VARCHAR(255),
    age INT,
    gender VARCHAR(255),
    presence_percentage DECIMAL(5, 2)
);
"""
cursor.execute(create_table_query)

create_table_query = """
CREATE TABLE IF NOT EXISTS 500k ( 
    student_id INT PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    grade INT,
    result VARCHAR(255),
    age INT,
    gender VARCHAR(255),
    presence_percentage DECIMAL(5, 2)
);
"""
cursor.execute(create_table_query)

create_table_query = """
CREATE TABLE IF NOT EXISTS 750k ( 
    student_id INT PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    grade INT,
    result VARCHAR(255),
    age INT,
    gender VARCHAR(255),
    presence_percentage DECIMAL(5, 2)
);
"""
cursor.execute(create_table_query)

create_table_query = """
CREATE TABLE IF NOT EXISTS 250k ( 
    student_id INT PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    grade INT,
    result VARCHAR(255),
    age INT,
    gender VARCHAR(255),
    presence_percentage DECIMAL(5, 2)
);
"""
cursor.execute(create_table_query)


csv_files = [
    'E:\\datasets\\1million.csv',
    'E:\\datasets\\750k.csv',
    'E:\\datasets\\500k.csv',
    'E:\\datasets\\250k.csv'
]

engine = create_engine('mysql+mysqlconnector://root:ARYA_2003@localhost:3305/students_db')

for i, csv_file_path in enumerate(csv_files):
    table_name = ['1million', '750k', '500k', '250k'][i]
    df = pd.read_csv(csv_file_path)

    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data inserted successfully into table: {table_name}")
    except Exception as e:
        print(f"Error inserting data into table: {table_name}. Error: {str(e)}")

connection.commit()

cursor.close()
connection.close()
