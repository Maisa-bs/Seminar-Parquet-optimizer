import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import time
from queries import get_queries




class DuckDb:
	def __init__(self):
	      # Connect to a DuckDB database
		self._conn = duckdb.connect('mydatabase.duckdb')

		# Create a cursor to execute SQL queries
		cursor = self._conn.cursor()


	def get_tpch_queries_indices(self):
		return 	[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22]

	def process_query(self, query):
		query_time = 0
		for i in range(5):
			self._conn.execute("VACUUM")			
			# Execute an SQL query on the table
			
			start = time.perf_counter()
			result = self._conn.execute(query)
			result_df = result.fetchdf()
			print(f"result = {result_df}")
			end = time.perf_counter()
			query_time += end - start


		return query_time / 5

	

	def process_tpch_query(self, index_query, option, pathdir):
		self._queries= get_queries(f"'{pathdir}customer/*.parquet'", f"'{pathdir}lineitem/*.parquet'", f"'{pathdir}nation/*.parquet'", f"'{pathdir}orders/*.parquet'", f"'{pathdir}part/*.parquet'", f"'{pathdir}partsupp/*.parquet'",f"'{pathdir}region/*.parquet'", f"'{pathdir}supplier/*.parquet'")                

		query = self._queries[index_query-1]
		query_time = 0
		for i in range(5):
			self._conn.execute("VACUUM")			
			# Execute an SQL query on the table
			
			start = time.perf_counter()
			result = self._conn.execute(query)
			result_df = result.fetchdf()
			end = time.perf_counter()
			query_time += end - start
			print(f"result = {result_df}")


		return query_time / 5


'''
duckdb = DuckDb()
time = duckdb.process_query("select count(*) from './output_data_duplicates/encoding/plain/duplicates/*.parquet' where age>=20")

print(time)

'''