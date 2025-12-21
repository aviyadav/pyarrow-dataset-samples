import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import date

def projection_pushdown_example(dataset):
    cols = ["auction_id", "country", "price", "ts"]
    
    scanner = dataset.scanner(columns=cols)
    table = scanner.to_table()
    
    print(table)


def predicate_pushdown_example(dataset):
    cols = ["auction_id", "country", "price", "ts"]
    f = (pc.field("country") == "AU") & (pc.field("price") > 100)
    
    scanner = dataset.scanner(columns=cols, filter=f)
    table = scanner.to_table()
    
    print(table)


def partition_pruning(dataset):
    schema = pa.schema([
        pa.field("country", pa.string()),
        pa.field("ds", pa.date32())
    ])

    f = (pc.field("country") == "DE") & (pc.field("ts") >= pa.scalar(date(2025,10,1)))

    tbl = dataset.to_table(
        columns=["auction_id", "price", "ts", "country", "ds"],
        filter=f
    )

    print(tbl)
    row_group_layout(tbl)

def row_group_layout(table):
    parquet_opts = ds.ParquetFileFormat().make_write_options(compression="zstd", use_dictionary=True)
    ds.write_dataset(
        data=table,
        base_dir="./analytics/bids-row-group-layout",
        format="parquet",
        partitioning=["country", "ds"],
        existing_data_behavior="overwrite_or_ignore",
        file_options=parquet_opts,
        max_rows_per_file=5_000_000,
        max_rows_per_group=256_000
    )

def batch_stream(dataset):
    scanner = dataset.scanner(
        columns=["auction_id", "price", "ts"],
        filter=pc.field("price") > 25,
        use_threads=True,          # let Arrow parallelize IO/decoding
        batch_size=64_000
    )

    total = 0
    for batch in scanner.to_batches():
        # process batch -> e.g., to polars, or write out
        total += len(batch)
    print(total)

def main(dataset):
    # projection_pushdown_example(dataset)
    # predicate_pushdown_example(dataset)
    # partition_pruning(dataset)
    batch_stream(dataset)


if __name__ == "__main__":
    dataset = ds.dataset("./analytics/bids/", format="parquet", partitioning="hive")
    
    main(dataset)
