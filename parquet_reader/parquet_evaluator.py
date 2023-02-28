from parquet_writer import ParquetWriter
from parquet_reader.spark_reader import Spark
from parquet_reader.duckdb_reader import DuckDb
from parquet_reader.drill_reader import Drill
from queries import NUM_QUERIES, QUERY_DICT, QUERIES_INDEX
import matplotlib.pyplot as plot
import numpy as np
import os



class ParquetEvaluator:
	def __init__(self):
		self._writer = ParquetWriter()
		self._spark_reader = Spark()
		self._duckdb_reader = DuckDb()
		self._drill_reader = Drill()



	def _get_size(self, folder_path):
		total_size = 0
		for dirpath, dirnames, filenames in os.walk(folder_path):
			for f in filenames:
				fp = os.path.join(dirpath, f)
				print(fp)
				total_size += os.path.getsize(fp)
		return total_size / (1024 * 1024)

		

	def evaluate_options_with_reader(self, pathdir, option_key, option_values, reader, query):
		class_ = globals()[reader]
		parquet_reader = class_() 

		if not(query):
			queries_indices = parquet_reader.get_tpch_queries_indices()
			num_queries =  len(queries_indices)
		else : 
			queries_indices = [query]
			num_queries =  1	

		print(f"queries_indices = {queries_indices}, num_queries = {num_queries}")
		times = [[] for i in range(num_queries)] 
		sizes = []

		for option in option_values: 
			folder_dir = f"{pathdir}/{option_key}/{option}/"
			sizes.append(self._get_size(folder_dir))

		for (i,query_index) in enumerate(queries_indices):
			print(f"query_index = {query_index}")
			for option in option_values:
				print(f"option = {option}")
				folder_dir = f"{pathdir}/{option_key}/{option}/"
				time = parquet_reader.process_tpch_query(query_index, option, folder_dir) 
				times[i].append(time)

		print(f"times = {times}\n")
		print(f"sizes = {sizes}")


		self.plot_results(times, sizes, option_key, option_values, reader, queries_indices)

		return times

	def plot_results(self, times, sizes, option_key, option_values, reader, queries_indices):
		plot.figure(figsize=(6,3))
		plot.rcParams.update({'font.size': 18})
		plot.subplot(1, 1, 1)

		X_axis = np.arange(len(queries_indices))
		X_labels = [ f"Q{i}" for i in queries_indices]
		# create bar charts


		time_1 = [t[0] for t in times]
		time_2 = [t[1] for t in times]
		time_3 = [t[2] for t in times]
		#time_4 = [t[3] for t in times]

		times_avg = [np.sum(time_1), np.sum(time_2), np.sum(time_3)]

		label_1 = f"{option_values[0]} {sizes[0]:.2f}MB"
		label_2 = f"{option_values[1]} {sizes[1]:.2f}MB"
		label_3 = f"{option_values[2]} {sizes[2]:.2f}MB"
		#label_4 = f"{option_values[3]} {sizes[3]:.2f}MB"

		plot.bar(X_axis - 0.2, time_1, 0.2, label=label_1, color= "#e9c46a")
		plot.bar(X_axis , time_2 , 0.2, label=label_2, color ="#2a9d8f" )
		#plot.bar(X_axis + 0.1, time_3, 0.2, label=label_3, color= "#264653")
		plot.bar(X_axis + 0.2, time_3, 0.2, label=label_3, color= "#e76f51")


		plot.xticks(X_axis, X_labels)
		# add labels and title
		plot.ylabel('time(s)')
		plot.legend()
		plot.gca().set_title(f"{reader} Evaluation")
		print(f"Time average: {times_avg}")
		plot.show()



	def evaluate_options(self, pathdir, option_key, option_values):
		#self._writer.write_tpch_parquet(pathdir, option_key, option_values)

		spark_times = [[] for i in range(NUM_QUERIES)] 
		duckdb_times = [[] for i in range(NUM_QUERIES)] 
		drill_times = [[] for i in range(NUM_QUERIES)] 

		sizes = []

		tables = ["customer", "orders", "lineitem", "supplier", "nation", "region", "part", "partsupp"]
		for option in option_values: 
			print(f"option = {option}")				
			size = 0

			for table in tables: 
				folder_dir = f"{pathdir}/{option_key}/{option}/"
				table_dir = f"{folder_dir}{table}/"
				size += self.__get_size(table_dir)

			sizes.append(size)


		for (i,index)  in enumerate(QUERIES_INDEX):

			tables = QUERY_DICT[index]

			print(f"query = {index}")

			for option in option_values: 
				print(f"option = {option}")				
				size = 0
				folder_dir = f"{pathdir}/{option_key}/{option}/"
				table_dir = f"{folder_dir}{table}/"


				spark_time = self._spark_reader.process_query(index - 1,option,folder_dir)
				duckdb_time = self._duckdb_reader.process_query(index -1, option, folder_dir)  
				#drill_time = self._drill_reader.process_query(index - 1, option, folder_dir)  
				spark_times[i].append(spark_time)
				duckdb_times[i].append(duckdb_time)
				#drill_times[i].append(drill_time)
		
		print(f"spark_times = {spark_times}")                
		print(f"duckdb_times = {duckdb_times}")   
		#print(f"drill_times = {drill_times}")                
		print(f"sizes = {sizes}")   
	   
		return (sizes, spark_times, duckdb_times, drill_times)


	def plot_results_per_reader(self, option_values, sizes, spark_times, duckdb_times, drill_times):
		sizes = [6.1, 2.4, 3.4, 2.6]
		plot.subplot(2, 1, 1)

		X_axis = np.arange(NUM_QUERIES)
		X_labels = [ f"Q{i}" for i in QUERIES_INDEX]
		# create bar charts


		spark_1 = [t[0] for t in spark_times]
		spark_2 = [t[1] for t in spark_times]
		spark_3 = [t[2] for t in spark_times]
		spark_4 = [t[3] for t in spark_times]

		sparks = [np.mean(spark_1), np.mean(spark_2), np.mean(spark_3), np.mean(spark_4)]

		print(f"sparks average: {sparks}")


		label_1 = f"none {sizes[0]}GB"
		label_2 = f"gzip {sizes[1]}GB"
		label_3 = f"snappy {sizes[2]}GB"
		label_4 = f"zstd {sizes[3]}GB"

		plot.bar(X_axis - 0.3, spark_1, 0.2, label=label_1, color= "#e9c46a")
		plot.bar(X_axis - 0.1, spark_2, 0.2, label=label_2, color ="#2a9d8f" )
		plot.bar(X_axis + 0.1, spark_3, 0.2, label=label_3, color= "#264653")
		plot.bar(X_axis + 0.3, spark_4, 0.2, label=label_4, color= "#e76f51")



		plot.xticks(X_axis, X_labels)
		# add labels and title
		plot.ylabel('time(s)')
		plot.legend()

		plot.gca().set_title(f"Spark Evaluation")
		
		plot.subplot(2, 1, 2)

		duckdb_1 = [t[0] for t in duckdb_times]
		duckdb_2 = [t[1] for t in duckdb_times]
		duckdb_3 = [t[2] for t in duckdb_times]
		duckdb_4 = [t[3] for t in duckdb_times]

		ducks = [np.mean(duckdb_1), np.mean(duckdb_2), np.mean(duckdb_3), np.mean(duckdb_4)]

		print(f"ducks average: {ducks}")
		plot.bar(X_axis - 0.3, duckdb_1, 0.2, label=label_1, color= "#e9c46a")
		plot.bar(X_axis - 0.1, duckdb_2, 0.2, label=label_2, color ="#2a9d8f" )
		plot.bar(X_axis + 0.1, duckdb_3, 0.2, label=label_3, color= "#264653")
		plot.bar(X_axis + 0.3, duckdb_4, 0.2, label=label_4, color= "#e76f51")


		plot.xticks(X_axis, X_labels)
		# add labels and title
		plot.ylabel('time(s)')
		plot.legend()
		plot.gca().set_title(f"DuckDB Evaluation")
		plot.show()



'''
		plot.subplot(3, 1, 3)
		drill_1 = [t[0] for t in drill_times]
		dril_2 = [t[1] for t in drill_times]
		drill_3 = [t[2] for t in drill_times]
		drill_4 = [t[3] for t in drill_times]
		drills = [np.mean(drill_1), np.mean(dril_2), np.mean(drill_3), np.mean(drill_4)]

		print(f"drills average: {drills}")
		
		plot.bar(X_axis - 0.3, drill_1, 0.2, label=label_1, color= "#e9c46a")
		plot.bar(X_axis - 0.1, dril_2, 0.2, label=label_2, color ="#2a9d8f" )
		plot.bar(X_axis + 0.1, drill_3, 0.2, label=label_3, color= "#264653")
		plot.bar(X_axis + 0.3, drill_4, 0.2, label=label_4, color= "#e76f51")


		plot.xticks(X_axis, X_labels)

		# add labels and title
		plot.ylabel('time(s)')
		plot.legend()
		plot.gca().set_title(f"Drill Evaluation")
		plot.subplots_adjust(hspace=0.3)
'''





'''
		plot.subplot(3, 1, 2)
		plot.bar(X_axis - 0.3, spark_times[1], 0.2, label='Spark', color= "#FFAEBC")
		plot.bar(X_axis - 0.1, duckdb_times[1], 0.2, label='DuckDB', color ="#FBE7C6" )
		plot.bar(X_axis + 0.1, drill_times[1], 0.2, label='Drill', color= "#A0E7E5")
	  
		plot.xticks(X_axis, X_labels)

		# add labels and title
		plot.ylabel('Query 6 time')
		plot.legend()
'''



'''
csv_file_path = "./input_data/lineitem.tbl"

pathdir = "./output_data"
compression_key = "compression"
compression_values =  ["none"]

encoding_key = "encoding"
encoding_values = ["plain","rle", "rle_dictionary"]


evaluator = ParquetEvaluator()


sizes, spark_times, duckdb_times, drill_times= evaluator.evaluate_options(csv_file_path, pathdir, compression_key, compression_values)

evaluator.plot_results(compression_values, sizes, spark_times, duckdb_times, drill_times)


sizes, spark_times, duckdb_times, drill_times= evaluator.evaluate_options(csv_file_path, pathdir, encoding_key, encoding_values)

evaluator.plot_results(encoding_values, sizes, spark_times, duckdb_times, drill_times)



evaluator.evaluate_options(csv_file_path2, pathdir2, compression_key, compression_values)



writer = ParquetWriter() 
evaluator = ParquetEvaluator()

pathdir = "./output_data_new"



encoding_key = "encoding"
encoding_values = ["plain","rle", "rle_dictionary"]


writer.write_tpch_parquet(pathdir, encoding_key, encoding_values)


sizes, spark_times, duckdb_times, drill_times= evaluator.evaluate_options( pathdir, page_size_key, page_size_values)

evaluator.plot_results_per_reader(page_size_values, sizes, spark_times, duckdb_times, drill_times)


evaluator.plot_results_per_query(page_size_values, sizes, spark_times, duckdb_times, drill_times)


encoding_key = "encoding"
encoding_values = ["plain","rle", "rle_dictionary"]


compression_key = "compression"
compression_values = ["none","gzip", "snappy", "zstd"]


block_size_key = "blockSize"
block_size_values = [1024, 1024*1024, 128*1024*1024, 1024*1024*1024]

page_size_key = "pageSize"
page_size_values = [1024, 1024*1024, 1024*1024*1024, 1024*1024*1024*1024 ]

output_path = "./output_data"

writer.write_tpch_parquet(output_path, compression_key, compression_values)

writer.write_tpch_parquet(output_path, encoding_key, encoding_values)
writer.write_tpch_parquet(output_path, block_size_key, block_size_values)
writer.write_tpch_parquet(output_path, page_size_key, page_size_values)
'''



'''
import csv

with open('duplicate_values.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["value"])
    for i in range(10000000):
        writer.writerow(["duplicate"])


writer = ParquetWriter() 
evaluator = ParquetEvaluator()

pathdir = "./output_data_new"

input_path = "./duplicate_values.csv"


encoding_key = "encoding"
encoding_values = "plain"

writer.write_parquet(input_path, pathdir, encoding_key, encoding_values, "duplicates")





page_size_key = "pageSizee"
page_size_values = [2*1024*1024, 4*1024*1024, 256*1024*1024, 1024*1024*1024 ]

output_path = "./output_data_n_page"


evaluator = ParquetEvaluator()


sizes, spark_times, duckdb_times, drill_times= evaluator.evaluate_options(output_path, page_size_key, page_size_values)

evaluator.plot_results_per_reader(page_size_values, sizes, spark_times, duckdb_times, drill_times)


output_path = "./output_data_1"
evaluator = ParquetEvaluator()
compression_key = "compression"
compression_values = ["none","gzip", "snappy", "zstd"]
#evaluator.evaluate_options_with_reader(pathdir, compression_key, compression_values,"DrillReader")
sizes, spark_times, duckdb_times, drill_times= evaluator.evaluate_options(output_path, compression_key, compression_values)

evaluator.plot_results_per_reader(compression_values, sizes, spark_times, duckdb_times, drill_times)

'''

'''


evaluator = ParquetEvaluator()
writer = ParquetWriter()
encoding_key = "encoding"
encoding_values = ["plain","rle", "rle_dictionary"]
input_path = "./duplicates.csv"
pathdir = "./output_data_duplicates"

writer.configure_bulk_options(input_path, pathdir, encoding_key, encoding_values, "duplicates")
'''