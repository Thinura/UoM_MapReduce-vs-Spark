from pyhive import hive
import time
import pandas as pd
import csv
import os
from tabulate import tabulate
import matplotlib.pyplot as plt
from dotenv import load_dotenv

# Number of times to run the queries
number_iterations = 5

# Queries to run
queries = [
    "SELECT Year, avg((CarrierDelay / ArrDelay) * 100) FROM DelayedFlights GROUP BY Year ORDER BY Year",
    "SELECT Year, avg((NASDelay / ArrDelay) * 100) FROM DelayedFlights GROUP BY Year ORDER BY Year",
    "SELECT Year, avg((WeatherDelay / ArrDelay) * 100) FROM DelayedFlights GROUP BY Year ORDER BY Year",
    "SELECT Year, avg((LateAircraftDelay / ArrDelay) * 100) FROM DelayedFlights GROUP BY Year ORDER BY Year",
    "SELECT Year, avg((SecurityDelay / ArrDelay) * 100) FROM DelayedFlights GROUP BY Year ORDER BY Year"
]

queries_names = [
    "Carrier Delay Query",
    "NAS Delay Query",
    "Weather Delay Query",
    "Late Aircraft Delay Query",
    "Security Delay Query"
]

load_dotenv()

# Connection parameters
hive_host = os.getenv("HIVE_HOST")
hive_port = 10000  # Default Hive server port on EMR
username = 'hadoop'
password = None  # No password required by default

# Establish connection
connection = hive.Connection(host=hive_host, port=hive_port, username=username, password=password)

# Create cursor
cursor = connection.cursor()

# Dictionary to store timing data
timing_data = {query: {} for query in queries}
query_results = {query: {} for query in queries}

# Execute queries multiple times
for iteration in range(number_iterations):
    iteration_key = f'MapReduce {iteration + 1}'
    for query_number, query in enumerate(queries, start=1):
        start_time = time.time()
        cursor.execute(query)
        end_time = time.time()
        results = cursor.fetchall()
        # Filter out tuples with None values
        results = [row for row in results if all(val is not None for val in row)]
        # Organize query results year-wise
        for row in results:
            year = row[0]
            result = row[1]
            # Check if year is already present in query_results[query]
            if year not in query_results[query]:
                query_results[query][year] = [result]
            else:
                # Check if a result is already present for the year
                if result not in query_results[query][year]:
                    query_results[query][year].append(result)

        # Initialize timing_data[query] if not already initialized
        if iteration_key not in timing_data[query]:
            timing_data[query][iteration_key] = []

        # Append timing data to the dictionary
        timing_data[query][iteration_key].append(end_time - start_time)
        print(f"In iteration {iteration+1}, Time Taken to execute query {query_number}: {end_time - start_time} seconds")

# Close cursor and connection
cursor.close()
connection.close()

print(F"Timing Data: {timing_data}")
print(F"Query Data: {query_results}")

# Initialize data dictionary
data = {'Query': []}
num_iterations = len(next(iter(timing_data.values())))
for i in range(1, num_iterations + 1):
    data[f'MapReduce {i}'] = []
data['Sum'] = []
data['Average'] = []

# Restructuring the dictionary
for query, mapreduce_data in timing_data.items():
    data['Query'].append(query)
    for i in range(1, num_iterations + 1):
        mapreduce_key = f'MapReduce {i}'
        if mapreduce_key in mapreduce_data:
            data[mapreduce_key].append(mapreduce_data[mapreduce_key][0])
        else:
            data[mapreduce_key].append(None)

# Calculate sum and average
for i in range(len(data['Query'])):
    sum_value = sum(
        data[f'MapReduce {j}'][i] for j in range(1, num_iterations + 1) if data[f'MapReduce {j}'][i] is not None)
    data['Sum'].append(sum_value)
    data['Average'].append(sum_value / num_iterations)

# Create DataFrame
df = pd.DataFrame(data)

# Calculate sum for each column
sum_row = df.sum()

# Append sum row to the DataFrame
df = pd.concat([df, sum_row.to_frame().T], ignore_index=True)

# Set the 'Query' value for the last row to an empty string
df.at[len(df) - 1, 'Query'] = 'SUM'

# Display DataFrame
print(tabulate(df, headers='keys', tablefmt='grid'))


# Function to render DataFrame as table plot and export as PNG
def export_table_as_png(df, filename, title=None, fontsize=14):
    # Create a new figure and axis with the specified figure size
    fig, ax = plt.subplots()

    # Hide axes
    ax.axis('tight')
    ax.axis('off')

    # Plot the DataFrame as a table with increased font size
    table = ax.table(cellText=df.values, colLabels=df.columns, loc='center', fontsize=fontsize)

    # Adjust font properties
    for key, cell in table.get_celld().items():
        cell.set_fontsize(fontsize)

    table.auto_set_column_width(col=list(range(len(df.columns))))  # Adjust the column width
    table.auto_set_font_size(False)
    table.set_fontsize(fontsize)  # Adjust the font size as needed

    # Adjust column widths based on maximum width of content
    cell_dict = table.get_celld()
    for i in range(len(df.columns)):
        key = (0, i)
        width = max([len(str(cell_dict[(j, i)].get_text().get_text())) for j in range(len(df.index))])
        cell_dict[key].set_width(width)

    ax.set_title(title, fontsize=fontsize * 1.5)  # Set the title

    # Save the plot as a PNG image
    plt.savefig(filename, bbox_inches='tight', pad_inches=1, dpi=300)


# Export DataFrame as table plot
export_table_as_png(df, './map_reduce/tables/time_data_query_results.png', title="Time Data Query", fontsize=6)

# Save DataFrame to CSV
df.to_csv('./map_reduce/csv/time_data_query_results.csv', index=False)

# Initialize empty lists to store data
query_labels = []
query_data = []

# Iterate over each query and extract data
for query, data in query_results.items():
    query_labels.append(query)
    query_data.append(sorted(data.items()))

for i, (query, data) in enumerate(query_results.items(), start=1):
    years = [year for year, _ in data.items()]
    avg_delay = [value[0] for _, value in data.items()]
    df = pd.DataFrame({'Year': years, f'Query_{i}_Average_Delay': avg_delay})
    export_table_as_png(df, f'./map_reduce/tables/Query_{i}_query_results.png', title=query, fontsize=6)

# Write data to a single CSV file
with open("./map_reduce/csv/query_results.csv", 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Query', 'Year', 'Result'])
    for query, results in query_results.items():
        writer.writerow([query, '', ''])  # Empty row to separate queries
        for year, result_list in results.items():
            for result in result_list:
                writer.writerow(['', year, result])
