import pyarrow.parquet as pq

def get_parquet_metadata(file_path): 
    parquet_file = pq.ParquetFile(file_path)
    print(parquet_file.metadata.row_group(0).column(0))


def get_parquet_encoding(file_path): 
    parquet_file = pq.ParquetFile(file_path)
    print(f"Encoding: {parquet_file.metadata.row_group(0).column(0).encodings}")


def get_parquet_compression(file_path):
    parquet_file = pq.ParquetFile(file_path)
    compression = parquet_file.metadata.row_group(0).column(0).compression
    compressed_size= parquet_file.metadata.row_group(0).column(0).total_compressed_size
    uncompressed_size= parquet_file.metadata.row_group(0).column(0).total_uncompressed_size
    compression_ratio = (compressed_size / uncompressed_size ) * 100 
    print(f"Compression: {compression}\n")
    print(f"Compressed size: {compressed_size}\n")
    print(f"Uncompressed size: {uncompressed_size}\n")
    print(f"Compression ratio: {compression_ratio:.2f}")


def get_parquet_stats(file_path):
    parquet_file = pq.ParquetFile(file_path)
    print(f"Statistics: {parquet_file.metadata.row_group(0).column(0).statistics}\n")


