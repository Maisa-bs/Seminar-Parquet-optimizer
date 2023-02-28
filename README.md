# Parquet Format Optimization

The project is implemented as part of the "Database Systems Seminar" during the winter semester of 2022/2023. The course is part of the Elite Software Engineering Graduate Program, offered by the University of Augsburg, the Technical University of Munich, and the Ludwig-Maximilian University. The project is created by Maisa Ben Salah and supervised by Alice Rey M.Sc. 
It serves as a tool to write parquet files using different formatting options and process the stored data using different formatting options.

## configure.py: Parquet Formatting options Configurator

The `configure.py` script bundles a set of utilities that facilitate configurating the formatting options when reading and writing parquet files. 

## Getting started
### Requirements

The requirements for the script are in the `environment.yml` file. To create the conda environment using the yml file, run `conda env create -f environment.yml`.


### Setup 
To setup the connection to Apache Drill client, the following development variables need to be set:

First, 
 `dfs.root` should point to the directory containing the parquet files on which the SQl queries are executed.
 
 Second, the following parameters should be set: 
```SQL
SET `store.parquet.reader.int96_as_timestamp` = TRUE;
SET `planner.enable_nljoin_for_scalar_only` = TRUE;
```

If not set, the TPCH queries won't be executed correctly.

On the other hand, no further setup for Spark and DuckDB is required.

Second, prepare the TPC-H Data. For that: 
* On Linux machine:
```
git clone https://github.com/gregrahn/tpch-kit.git
cd tpch-kit/dbgen
make MACHINE=LINUX DATABASE=POSTGRESQ
```
* On MacOS machine:
```
git clone https://github.com/gregrahn/tpch-kit.git
cd tpch-kit/dbgen
make MACHINE=MACOS DATABASE=POSTGRESQL
```


Set DSS_PATH variable to specify where the generated data will be stored
(directory has to exist already)
```
export DSS_PATH=/path/to/tpc-h/data/directory
```
Generate the TPC-H Data with a scaling factor
```
cd /path/to/tpch-kit/repo/dbgen
./dbgen -s 1
```

### Utilities overview

```
$ python configure.py --help
Usage: manage.py [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  evaluate-parquet-reader  Evaluate formatting options for a Parquet reader
  profile-parquet          Profile a specific parquet file
  write-parquet            Write data in Parquet file
  write-tpch-parquet       Write tpch data in Parquet files
```

Running `python configure.py COMMAND --help` will display its description.

The Parquet readers that are supported are the follwing: 
* Apache Spark (Spark)
* Apache Drill (Drill)
* DuckDB (DuckDb.py)

Formatting options: 
* compression: none, snappy, gzip, zstd
* encoding: plain, rle, rle_dictionary
* blockSize 
* PageSize


## Getting Started
This is an overview of the different subsystems:
- [Parquet Writer](parquet_writer.py): converts *.csv or *.TBL files to Parquet files using the specified formatting options.
- [reader](parquet_reader): executes SQL queries on the Parquet files: [Apache Spark](spark_reader.py), [Apache Drill](drill_reader.py), and [DuckDB](duckdb_reader.py). 
- [profiler](parquet_profiler.py): checks the formatting options of the parquet files.
