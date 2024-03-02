import csv
import matplotlib.pyplot as plt
import numpy as np

def process_data(data):
    first_values = []
    averages = []

    for sublist in data:
        first_value = float(sublist[0])
        first_values.append(first_value)

        remaining_values = [float(value) for value in sublist[1:]]
        average = sum(remaining_values) / len(remaining_values)
        averages.append(average)

    return first_values, averages

def process_csv_file(file_path):
    all_values = []

    with open(file_path, 'r') as f:
        csv_reader = csv.reader(f)
        next(csv_reader)

        for row in csv_reader:
            values = [float(value) for value in row[1:]]
            all_values.append(values)

    return all_values

def plot_bar_plot(datasets, response_times, xlabel, ylabel, title):
    plt.figure(figsize=(4, 5))
    plt.bar(datasets, response_times, width=0.1)
    plt.yscale('log')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True)
    plt.show()

def plot_grouped_bar_plot(response_times, databases, dataset_sizes, xlabel, ylabel, title):
    plt.figure(figsize=(10, 10))
    bar_width = 0.15
    space_between_bars = 0
    index = np.arange(len(response_times[0]))

    for i, times in enumerate(response_times):
        plt.bar(index + (bar_width + space_between_bars) * i, times,
                bar_width, alpha=0.7, label=f'Dataset {dataset_sizes[i]}')

    plt.yscale('log')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)

    plt.xticks([])

    plt.legend()
    plt.grid(True)

    cell_text = []
    for i, times in enumerate(response_times):
        row = [f'{int(val)}' for val in times]
        cell_text.append(row)

    table = plt.table(cellText=cell_text,
                      rowLabels=[f'Dataset {dataset_sizes[i]}' for i in range(len(response_times))],
                      colLabels=databases,
                      loc='bottom')

    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1, 1.5)

    plt.tight_layout()
    plt.show()

file_mysql = [
    "E:\\query_results\\MySQL\\results_1million.csv",
    "E:\\query_results\\MySQL\\results_250k.csv",
    "E:\\query_results\\MySQL\\results_500k.csv",
    "E:\\query_results\\MySQL\\results_750k.csv"
]

all_values_mysql = [process_csv_file(file) for file in file_mysql]
flat_all_values_mysql = [item for sublist in all_values_mysql for item in sublist]  # Flatten the list
first_values_mysql, average_of_theRest_mysql = process_data(flat_all_values_mysql)


file_redis = [
    "E:\\query_results\\redis\\results_1million.csv",
    "E:\\query_results\\redis\\results_250k.csv",
    "E:\\query_results\\redis\\results_500k.csv",
    "E:\\query_results\\redis\\results_750k.csv"
]

all_values_redis = [process_csv_file(file) for file in file_redis]
flat_all_values_redis = [item for sublist in all_values_redis for item in sublist]
first_values_redis, average_of_theRest_redis = process_data(flat_all_values_redis)

file_cassandra = [
    "E:\\query_results\\cassandra\\results_1million.csv",
    "E:\\query_results\\cassandra\\results_250k.csv",
    "E:\\query_results\\cassandra\\results_500k.csv",
    "E:\\query_results\\cassandra\\results_750k.csv"
]

all_values_cassandra = [process_csv_file(file) for file in file_cassandra]
flat_all_values_cassandra = [item for sublist in all_values_cassandra for item in sublist]
first_values_cassandra, average_of_theRest_cassandra = process_data(flat_all_values_cassandra)

file_mongodb = [
    "E:\\query_results\\mongoDB\\results_1million.csv",
    "E:\\query_results\\mongoDB\\results_250k.csv",
    "E:\\query_results\\mongoDB\\results_500k.csv",
    "E:\\query_results\\mongoDB\\results_750k.csv"
]

all_values_mongodb = [process_csv_file(file) for file in file_mongodb]
flat_all_values_mongodb = [item for sublist in all_values_mongodb for item in sublist]
first_values_mongodb, average_of_theRest_mongodb = process_data(flat_all_values_mongodb)

file_neo4j = [
    "E:\\query_results\\neo4j\\results_250k.csv",
    "E:\\query_results\\neo4j\\results_500k.csv",
    "E:\\query_results\\neo4j\\results_750k.csv",
    "E:\\query_results\\neo4j\\results_1million.csv"
]

all_values_neo4j = [process_csv_file(file) for file in file_neo4j]
flat_all_values_neo4j = [item for sublist in all_values_neo4j for item in sublist]
first_values_neo4j, average_of_theRest_neo4j = process_data(flat_all_values_neo4j)

dataset_sizes = ["250k", "500k", "750k", "1m"]

response_times = [
    [first_values_mysql[0], first_values_redis[0], first_values_cassandra[0], first_values_mongodb[0], first_values_neo4j[0]],
    [first_values_mysql[1], first_values_redis[1], first_values_cassandra[1], first_values_mongodb[1], first_values_neo4j[1]],
    [first_values_mysql[2], first_values_redis[2], first_values_cassandra[2], first_values_mongodb[2], first_values_neo4j[2]],
    [first_values_mysql[3], first_values_redis[3], first_values_cassandra[3], first_values_mongodb[3], first_values_neo4j[3]]
]

plot_grouped_bar_plot(response_times, ['mySql', 'redis', 'cassandra', 'mongodb', 'neo4j'], dataset_sizes, '', 'Response Time (ns)',
                      'Response Time for Different Databases and Datasets')
