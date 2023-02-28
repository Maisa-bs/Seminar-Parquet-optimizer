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
