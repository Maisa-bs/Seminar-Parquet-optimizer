from pyspark.sql import SparkSession,SQLContext
import time
from queries import QUERY_DICT, get_queries

class Spark:

	def __init__(self):
		self._queries = get_queries()

	def get_tpch_queries_indices(self):
		return 	[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22]

	def process_query(self, query,table_dir,table):
		query_time = 0
		for i in range(5):
			spark = SparkSession.builder.appName("SQL on Parquet").getOrCreate()
			sc=spark.sparkContext
			sqlContext = SQLContext(sc)
			# Load the Parquet file

			start = time.perf_counter()

			df = sqlContext.read.parquet(table_dir)
			df.createOrReplaceTempView(table)            
			
		
			# Execute the SQL query
			result = spark.sql(query)
			end = time.perf_counter()
			query_time += end - start 
			result.show()
			spark.catalog.clearCache()
		return query_time / 5



	def process(self, query, pathdir, query_tables):
		spark = SparkSession.builder.getOrCreate()
		sc=spark.sparkContext
		sqlContext = SQLContext(sc)
		# Load the Parquet file

		start = time.perf_counter()
		

		for table in query_tables:
			df = sqlContext.read.parquet(f"{pathdir}/{table}")
			df.createOrReplaceTempView(table)            
		
	
		# Execute the SQL query
		result = spark.sql(query)
		end = time.perf_counter()
		query_time = end - start 
		spark.catalog.clearCache()
		for table in query_tables:
			spark.catalog.dropTempView(table)
		spark.stop()

		return query_time

	def process_tpch_query(self, query_index, option, pathdir):
		query_time = 0
		query_tables = QUERY_DICT[query_index]

		query = self._queries[query_index - 1]
		if query_index == 1:
			self.process(query, pathdir, query_tables)

		for i in range(5):
			query_time += self.process(query, pathdir, query_tables)
			
		#result.show()
		return query_time / 5
		

'''
spark = Spark()
time = spark.process_query('select count(*) from duplicates where age>=20', './output_data_duplicates/encoding/plain/duplicates/', 'duplicates')

print(time)

'''