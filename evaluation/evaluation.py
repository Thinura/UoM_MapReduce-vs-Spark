import csv
import matplotlib.pyplot as plt
import pandas as pd

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

def get_query_average_times(csv_file, queries=None):
    if queries is None:
        queries = queries_names  # Assuming queries_names is defined somewhere in your code

    query_iteration_times = {query_name: [] for query_name in queries}
    with open(csv_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row_number, row in enumerate(reader):
            if row_number + 1 > len(queries):
                # If row_number exceeds the length of queries, break or handle as needed
                break
            query_name = queries[row_number]
            query_iteration_times[query_name].append(float(row['Average']))

    return query_iteration_times


def get_query_iteration_times(type, csv_file, iterations=number_iterations):
    if type not in ['MapReduce', 'Spark']:
        raise ValueError("Invalid type. Must be 'MapReduce' or 'Spark'.")

    query_iteration_times = {f'Iteration {iteration + 1}': [] for iteration in range(iterations)}
    with open(csv_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            for iteration in range(iterations):
                query_iteration_times[f'Iteration {iteration + 1}'].append(float(row[f'{type} {iteration + 1}']))

    return query_iteration_times


mapReduceQueryIterationTimes = get_query_iteration_times('MapReduce', '../map_reduce/csv/time_data_query_results.csv')
mapReduceQueryAverageTimes = get_query_average_times('../map_reduce/csv/time_data_query_results.csv', queries_names)
print(f'mapReduce {mapReduceQueryAverageTimes}')

sparkQueryIterationTimes = get_query_iteration_times('Spark', '../spark/csv/time_data_query_results.csv')
sparkQueryAverageTimes = get_query_average_times('../spark/csv/time_data_query_results.csv', queries_names)
print(f'spark {sparkQueryAverageTimes}')


def export_table_as_png(df, filename, title=None, fontsize=14):
    # Calculate the table size based on the number of rows and columns in the DataFrame
    table_height = len(df) + 1  # Add 1 for column labels
    table_width = len(df.columns)

    # Create a new figure with the calculated size
    fig, ax = plt.subplots(figsize=(table_width, table_height))

    # Hide axes
    ax.axis('tight')
    ax.axis('off')

    # Plot the DataFrame as a table with increased font size
    table = ax.table(cellText=df.values, colLabels=df.columns, loc='center', fontsize=fontsize)

    # Adjust font properties
    for key, cell in table.get_celld().items():
        cell.set_fontsize(fontsize)

    table.auto_set_column_width(col=list(range(table_width)))  # Adjust the column width
    table.auto_set_font_size(False)
    table.set_fontsize(fontsize)  # Adjust the font size as needed

    # Adjust column widths based on maximum width of content
    cell_dict = table.get_celld()
    for i in range(table_width):
        key = (0, i)
        width = max([len(str(cell_dict[(j, i)].get_text().get_text())) for j in range(len(df.index))])
        cell_dict[key].set_width(width)

    ax.set_title(title, fontsize=fontsize * 1.5)  # Set the title

    # Save the plot as a PNG image
    plt.savefig(filename, bbox_inches='tight', pad_inches=1, dpi=300)
    plt.close()


def plot_iteration_times(map_reduce_times, spark_times, filename=None):
    # Extracting iteration numbers and times for MapReduce and Spark
    iterations = list(map_reduce_times.keys())
    map_reduce_values = [value[0] for value in map_reduce_times.values()]
    spark_values = [value[0] for value in spark_times.values()]

    # Calculating the width dynamically based on the length of the title
    title = 'Heterogeneous Query Evaluation (MapReduce vs Spark) - Running Time vs Iteration'
    title_length = len(title)
    auto_width = 0.2 * title_length  # Adjust the coefficient as needed for the desired scaling

    # Plotting
    bar_width = 0.35
    index = range(len(iterations))
    plt.figure(figsize=(auto_width, 6))  # Adjust the height (6) as needed

    plt.bar(index, map_reduce_values, bar_width, label='MapReduce')
    plt.bar([i + bar_width for i in index], spark_values, bar_width, label='Spark')

    plt.xlabel('Iteration Number')
    plt.ylabel('Running Time (seconds)')
    plt.title(title)
    plt.xticks([i + bar_width / 2 for i in index], iterations)
    plt.legend()

    plt.tight_layout()

    # Save the plot if filename is provided
    if filename:
        plt.savefig(filename)
    else:
        plt.show()

    plt.close()


comparison_file_path = './charts/Heterogeneous_Query_Evaluation_(MapReduce_vs_Spark)_-_Running_Time_vs_Iteration.png'

# Run the query using Hadoop and Spark for 5 times and plot the graph in comparing both methods (running time vs iteration).
plot_iteration_times(mapReduceQueryIterationTimes, sparkQueryIterationTimes, comparison_file_path)

# Similarly process all queries and plot the time-comparison graphs as shown above.
data = {'Query': queries_names}
for query_name in queries_names:
    data.setdefault('MapReduce', []).extend(mapReduceQueryAverageTimes.get(query_name, []))
    data.setdefault('Spark', []).extend(sparkQueryAverageTimes.get(query_name, []))

print(data)
# Create DataFrame
df = pd.DataFrame(data)

# Export DataFrame as table plot.
export_table_as_png(df, f'./charts/Homogeneous_Query_Evaluation_(MapReduce_vs_Spark)_-_Total_Runtime_in_{number_iterations}_Iterations.png', title=f'Homogeneous Query Evaluation (MapReduce vs Spark) - Total Runtime in {number_iterations} Iterations', fontsize=6)


def plot_queries_iteration_times(map_reduce_average_times, spark_average_times, filename=None):
    # Extracting query names and average times for MapReduce and Spark
    iterations = list(map_reduce_average_times.keys())
    map_reduce_values = [value[0] for value in map_reduce_average_times.values()]
    spark_values = [value[0] for value in spark_average_times.values()]

    # Calculating the width dynamically based on the length of the title
    title = 'Homogeneous Query Evaluation (MapReduce vs Spark) - Average Runtime for Different Queries'
    title_length = len(title)
    auto_width = 0.2 * title_length  # Adjust the coefficient as needed for the desired scaling

    # Plotting
    bar_width = 0.35
    index = range(len(iterations))
    plt.figure(figsize=(auto_width, 6))  # Adjust the height (6) as needed

    plt.bar(index, map_reduce_values, bar_width, label='MapReduce')
    plt.bar([i + bar_width for i in index], spark_values, bar_width, label='Spark')

    plt.xlabel('Query Name')
    plt.ylabel('Average Running Time (seconds)')
    plt.title(title)
    plt.xticks([i + bar_width / 2 for i in index], iterations)
    plt.legend()

    plt.tight_layout()

    # Save the plot if filename is provided
    if filename:
        plt.savefig(filename)
    else:
        plt.show()

    plt.close()


average_comparison_file_path = './charts/Homogeneous_Query_Evaluation_(MapReduce_vs_Spark)_-_Average_Runtime.png'

# Calculate the average time taken by MapReduce and Spark for each query and plot the graph
plot_queries_iteration_times(mapReduceQueryAverageTimes, sparkQueryAverageTimes, average_comparison_file_path)
