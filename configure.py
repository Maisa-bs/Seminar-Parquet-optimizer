import sys
import os
from dataclasses import dataclass
from os import listdir
from os.path import isfile, join, splitext
from parquet_writer import ParquetWriter
import click
from parquet_reader.parquet_evaluator import ParquetEvaluator
from parquet_profiler import get_parquet_metadata, get_parquet_encoding, get_parquet_compression, get_parquet_stats


@click.group()
def configure():
    pass

def parse_values(ctx, param, value):
    if value is None:
        return []
    return value.split(' ')


@configure.command()
@click.option("-i", "--input", type=int, required=True, help="input directory for TPCH data")
@click.option("-o", "--output", type=str, required=True, help="Directory of the output parquet files")
@click.option("-k", "--key", type=str, required=True, help ="Formatting option key")
@click.option("-v", "--values", type=str, callback=parse_values, required=True, help ="Formatting option values")
def write_tpch_parquet(input,output, key, values):
    """
    Write tpch data in Parquet files
    """
    writer = ParquetWriter()
    writer.write_tpch_parquet(input, output, key, values)



@configure.command()
@click.option("-i", "--input", type=str, required=True, help="Directory of the input csv file")
@click.option("-o", "--output", type=str, required=True, help="Directory of the output directory for the parquet files")
@click.option("-t", "--table", type=str, required=True, help="Table name")
@click.option("-k", "--key", type=str, required=True, help ="Formatting option key")
@click.option("-v", "--values", type=str, callback=parse_values, required=True, help ="Formatting option values")
def write_parquet(input,output,table,key,values):
    """
    Write data in Parquet file
    """
    writer = ParquetWriter()
    df = writer.read_csvFile(input)
    writer.configure_bulk_options(df, output, table, key, values)


@configure.command()
@click.option("-r", "--reader", type=str, required=True, help="Name of the parquet reader: Spark, Drill, DuckDB")
@click.option("-d", "--directory", type=str, required=True, help="Directory of the parquet files")
@click.option("-k", "--key", type=str, required=True, help ="Formatting option key")
@click.option("-v", "--values", type=str, callback=parse_values, required=True, help ="Formatting option values")
@click.option("-q", "--query", type=int, default = 0, help="Specific query otherwise all queries are executed")
def evaluate_parquet_reader(reader,directory, key, values, query):
    """
    Evaluate formatting options for a Parquet reader
    """
    evaluator = ParquetEvaluator()
    evaluator.evaluate_options_with_reader(directory, key, values,reader, query)

        

@configure.command()
@click.option("-d", "--directory", type=str, required=True, help="Directory of the parquet file")
@click.option("-m", "--metadata",is_flag=True, required=False, help="Metadata of the parquet file")
@click.option("-e", "--encoding", is_flag=True, required=False, help="Encoding of the parquet file")
@click.option("-c", "--compression", is_flag=True, required=False, help ="Compression of the parquet file")
@click.option("-s", "--stats", is_flag=True, required=False, help ="Stats of the parquet file")
def profile_parquet(directory,metadata, encoding, compression, stats):
    """
    Profile a specific parquet file
    """
    if metadata : 
        get_parquet_metadata(directory)
    if encoding: 
        get_parquet_encoding(directory)
    if compression : 
        get_parquet_compression(directory)
    if stats :
        get_parquet_stats(directory)


if __name__ == "__main__":
    configure()  # pylint: disable=no-value-for-parameter