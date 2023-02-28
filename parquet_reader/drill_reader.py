import pydrill
from pydrill.client import PyDrill
from pandas import *
import time
from queries import get_queries




class Drill:
	def __init__(self):
		# connect to drill api
		self._drill = PyDrill(host='localhost', port=8047)
		print (self._drill.is_active())


	def get_tpch_queries_indices(self):
		return 	[1,3,5,6,7,11,12,13,14,15,17,18,20,21,22]


	def process_query(self, query):
		query_time = 0
		for i in range(5):
			try:
				start = time.perf_counter()  
				df = self._drill.query(query)
				end = time.perf_counter()
				query_time += end - start 
			except (ValueError, TypeError):
				self._drill = PyDrill(host='localhost', port=8047)

		print   (f"Drill result: {df.to_dataframe()}")

		return query_time / 5



	def process_tpch_query(self, query_index, option, pathdir):
		self._queries = get_queries(f"dfs.root.`{pathdir}customer`",f"dfs.root.`{pathdir}lineitem`", f"dfs.root.`{pathdir}nation`", f"dfs.root.`{pathdir}orders`",
		f"dfs.root.`{pathdir}part`",f"dfs.root.`{pathdir}partsupp`",f"dfs.root.`{pathdir}region`", f"dfs.root.`{pathdir}supplier`" )


		query = self._queries[query_index - 1]
		query_time = 0

		print(f"query = {query}")

		for i in range(5):
			try:
				start = time.perf_counter()  
				df = self._drill.query(query)
				end = time.perf_counter()
				query_time += end - start 
			except (ValueError, TypeError):
				self._drill = PyDrill(host='localhost', port=8047)

		print   (f"Drill result: {df.to_dataframe()}")


		return query_time / 5

'''

drill = Drill()
time = drill.process_query("select count(*) from dfs.root.`./output_data_duplicates/encoding/rle_dictionary/duplicates` where age>=20")

print(time)
'''

	  