import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct


class ParquetWriter:
	def __init__(self):
		self._spark = SparkSession.builder.getOrCreate()

	def read_csvFile(self, file_path,header):
		# Scheme is automatically inferred 
		df = self._spark.read.format('csv') \
					.option('header', header) \
					.option('delimiter', '|') \
					.option('inferSchema', 'true') \
					.load(file_path)
		df.show()
		df.printSchema()

		return df


	def configure_option(self, df, path, table_name, option_key,option_value):
		 df.write.mode("overwrite") \
		 	.option(option_key, option_value) \
			.parquet(f"{path}/{option_key}/{option_value}/{table_name}")


	def configure_bulk_options(self,df, path, table_name, option_key, option_values): 
		for option in option_values: 
			self.configure_option(df, path, table_name, option_key,option)  


	def write_parquet(self, csv_path, output_path, option_key,option_value, table_name): 
		df = self.read_csvFile(csv_path, 'true')
		self.configure_option(df, output_path, table_name, option_key,option_value)

	def write_tpch_parquet(self, input_path, output_path, option_key, option_values): 
		path_customer = f"{input_path}/customer.tbl"
		path_lineitem = f"{input_path}/lineitem.tbl"
		path_nation = f"{input_path}/nation.tbl"
		path_orders = f"{input_path}/orders.tbl"
		path_part = f"{input_path}/part.tbl"
		path_partsupp = f"{input_path}/partsupp.tbl"
		path_region = f"{input_path}/region.tbl"
		path_supplier = f"{input_path}/supplier.tbl"

		df_customer = self.read_csvFile(path_customer, 'false')
		df_lineitem = self.read_csvFile(path_lineitem, 'false')
		df_nation = self.read_csvFile(path_nation, 'false')
		df_orders = self.read_csvFile(path_orders, 'false')
		df_part= self.read_csvFile(path_part, 'false')
		df_partsupp = self.read_csvFile(path_partsupp, 'false')
		df_region = self.read_csvFile(path_region, 'false')
		df_supplier = self.read_csvFile(path_supplier, 'false')


		df_customer = self.rename_columns_tpch_customer(df_customer)
		df_lineitem = self.rename_columns_tpch_lineitem(df_lineitem)
		df_nation = self.rename_columns_tpch_nation(df_nation)
		df_orders = self.rename_columns_tpch_orders(df_orders)
		df_part= self.rename_columns_tpch_part(df_part)
		df_partsupp = self.rename_columns_tpch_partsupp(df_partsupp)
		df_region = self.rename_columns_tpch_region(df_region)
		df_supplier = self.rename_columns_tpch_supplier(df_supplier)


		self.configure_bulk_options(df_customer, output_path, "customer", option_key, option_values )
		self.configure_bulk_options(df_lineitem, output_path, "lineitem", option_key, option_values )
		self.configure_bulk_options(df_nation, output_path, "nation", option_key, option_values )
		self.configure_bulk_options(df_orders, output_path, "orders", option_key, option_values )
		self.configure_bulk_options(df_part, output_path, "part", option_key, option_values )
		self.configure_bulk_options(df_partsupp, output_path, "partsupp", option_key, option_values )
		self.configure_bulk_options(df_region, output_path, "region", option_key, option_values )
		self.configure_bulk_options(df_supplier, output_path, "supplier", option_key, option_values )


	def rename_columns_tpch_customer(self, df):
		# Rename the columns
		return df.withColumnRenamed("_c0", "C_CUSTKEY") \
			.withColumnRenamed("_c1", "C_NAME") \
			.withColumnRenamed("_c2", "C_ADDRESS") \
			.withColumnRenamed("_c3", "C_NATIONKEY") \
			.withColumnRenamed("_c4", "C_PHONE") \
			.withColumnRenamed("_c5", "C_ACCTBAL") \
			.withColumnRenamed("_c6", "C_MKTSEGMENT") \
			.withColumnRenamed("_c7", "C_COMMENT") 

	def rename_columns_tpch_lineitem(self, df):
		# Rename the columns
		return df.withColumnRenamed("_c0", "L_ORDERKEY") \
			.withColumnRenamed("_c1", "L_PARTKEY") \
			.withColumnRenamed("_c2", "L_SUPPKEY") \
			.withColumnRenamed("_c3", "L_LINENUMBER") \
			.withColumnRenamed("_c4", "L_QUANTITY") \
			.withColumnRenamed("_c5", "L_EXTENDEDPRICE") \
			.withColumnRenamed("_c6", "L_DISCOUNT") \
			.withColumnRenamed("_c7", "L_TAX") \
			.withColumnRenamed("_c8", "L_RETURNFLAG") \
			.withColumnRenamed("_c9", "L_LINESTATUS") \
			.withColumnRenamed("_c10", "L_SHIPDATE") \
			.withColumnRenamed("_c11", "L_COMMITDATE") \
			.withColumnRenamed("_c12", "L_RECEIPTDATE") \
			.withColumnRenamed("_c13", "L_SHIPINSTRUCT") \
			.withColumnRenamed("_c14", "L_SHIPMODE") \
			.withColumnRenamed("_c15", "L_COMMENT")

	def rename_columns_tpch_nation(self, df):
		# Rename the columns
		return df.withColumnRenamed("_c0", "N_NATIONKEY") \
			.withColumnRenamed("_c1", "N_NAME") \
			.withColumnRenamed("_c2", "N_REGIONKEY") \
			.withColumnRenamed("_c3", "N_COMMENT")

	def rename_columns_tpch_orders(self, df):
		# Rename the columns
		return df.withColumnRenamed("_c0", "O_ORDERKEY") \
			.withColumnRenamed("_c1", "O_CUSTKEY") \
			.withColumnRenamed("_c2", "O_ORDERSTATUS") \
			.withColumnRenamed("_c3", "O_TOTALPRICE") \
			.withColumnRenamed("_c4", "O_ORDERDATE") \
			.withColumnRenamed("_c5", "O_ORDERPRIORITY") \
			.withColumnRenamed("_c6", "O_CLERK") \
			.withColumnRenamed("_c7", "O_SHIPPRIORITY") \
			.withColumnRenamed("_c8", "O_COMMENT")        

	def rename_columns_tpch_part(self, df):
		# Rename the columns
		return df.withColumnRenamed("_c0", "P_PARTKEY") \
			.withColumnRenamed("_c1", "P_NAME") \
			.withColumnRenamed("_c2", "P_MFGR") \
			.withColumnRenamed("_c3", "P_BRAND") \
			.withColumnRenamed("_c4", "P_TYPE") \
			.withColumnRenamed("_c5", "P_SIZE") \
			.withColumnRenamed("_c6", "P_CONTAINER") \
			.withColumnRenamed("_c7", "P_RETAILPRICE") \
			.withColumnRenamed("_c8", "P_COMMENT")

	def rename_columns_tpch_partsupp(self, df):
		# Rename the columns
		return df.withColumnRenamed("_c0", "PS_PARTKEY") \
			.withColumnRenamed("_c1", "PS_SUPPKEY") \
			.withColumnRenamed("_c2", "PS_AVAILQTY") \
			.withColumnRenamed("_c3", "PS_SUPPLYCOST") \
			.withColumnRenamed("_c4", "PS_COMMENT")

	def rename_columns_tpch_region(self, df):
		# Rename the columns
		return df.withColumnRenamed("_c0", "R_REGIONKEY") \
			.withColumnRenamed("_c1", "R_NAME") \
			.withColumnRenamed("_c2", "R_COMMENT") \
			.withColumnRenamed("_c3", "C_NATIONKEY")

	def rename_columns_tpch_supplier(self, df):
		# Rename the columns
		return df.withColumnRenamed("_c0", "S_SUPPKEY") \
			.withColumnRenamed("_c1", "S_NAME") \
			.withColumnRenamed("_c2", "S_ADDRESS") \
			.withColumnRenamed("_c3", "S_NATIONKEY") \
			.withColumnRenamed("_c4", "S_PHONE") \
			.withColumnRenamed("_c5", "S_ACCTBAL") \
			.withColumnRenamed("_c6", "S_COMMENT")
		


