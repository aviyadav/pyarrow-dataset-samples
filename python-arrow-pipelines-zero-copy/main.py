import os
from datetime import datetime, timezone
from multiprocessing import Array

import tzdata

# Point PyArrow at the tzdata package already installed in the venv.
# On Windows, PyArrow cannot find the system timezone database, so it falls
# back to ARROW_TZDATA_DIR.  This must be set before any pyarrow import.
os.environ["ARROW_TZDATA_DIR"] = os.path.join(
    os.path.dirname(tzdata.__file__), "zoneinfo"
)

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds


def dataset_scanner_compute(dataset):

    # Pushdown projection + filter into the scan where possible
    scanner = dataset.scanner(
        columns=["user_id", "event_id", "event_name", "amount", "currency", "ts"],
        filter=(ds.field("amount") > 0)
        & (
            ds.field("ts")
            >= pa.scalar(
                datetime(2025, 1, 1, tzinfo=timezone.utc),
                type=pa.timestamp("us", tz="UTC"),
            )
        )
        & (ds.field("currency") == "USD"),
    )

    batches = scanner.to_batches()

    # Arrow-native compute (still columnar buffers)
    out_batches = []

    for batch in batches:
        amount = batch.column("amount")
        enriched = batch.append_column(
            "amount_usd", pc.multiply(amount, pa.scalar(1.0, pa.float64()))
        )
        # Derive a plain date string (YYYY-MM-DD) from ts for safe partitioning.
        # Avoid pc.strftime entirely — it triggers a tzdata lookup on Windows even
        # for timezone-naive timestamps.  Instead: cast tz-aware ts → date32 (UTC
        # epoch days, no tz database needed) → string "YYYY-MM-DD".
        date_col = pc.cast(pc.cast(batch.column("ts"), pa.date32()), pa.string())
        enriched = enriched.append_column("date", date_col)
        out_batches.append(enriched)

    result = pa.Table.from_batches(out_batches)
    ds.write_dataset(
        result, base_dir="data/clean_events/", format="parquet", partitioning=["date"]
    )


def duckdb_arrow_sql(dataset):
    events = ds.dataset("data/events/", format="parquet")

    con = duckdb.connect(":memory:")
    con.register("events", events)

    arrow_tbl = con.execute("""
        SELECT user_id, sum(amount) AS total
        FROM events
        WHERE amount > 0
        GROUP BY user_id
    """).fetch_arrow_table()

    return arrow_tbl


def polars_lazy_scan():
    import polars as pl
    import pyarrow.compute as pc

    lf = (
        pl.scan_parquet("data/events/*.parquet")
        .filter(pl.col("amount") > 0)
        .group_by("user_id")
        .agg(pl.col("amount").sum().alias("total"))
    )

    df = lf.collect()

    # Mostly zero copy Arrow export (with some type caveats)
    arrow_tbl = df.to_arrow()

    # Arrow compute step (example: apply a clamp)
    total = arrow_tbl["total"]
    arrow_tbl = arrow_tbl.set_column(
        arrow_tbl.schema.get_field_index("total"),
        "total",
        pc.max_element_wise(total, 0),
    )

    # convert arrow table to polars dataframe
    # pl.from_arrow consumes the Arrow buffers with zero copy
    df = pl.from_arrow(arrow_tbl)
    print(df)


def arrow_to_pandas():
    import pandas as pd

    # Create Arrow-first
    tbl = pa.table({"a": [1, 2, 3], "s": ["x", "y", "z"]})

    # Convert with Arrow-aware dtype behavior
    df = tbl.to_pandas(types_mapper=pd.ArrowDtype)
    print(df)

    # When you need NumPy, be strict:
    # ChunkedArray may span multiple non-contiguous buffers, so zero-copy to
    # NumPy is impossible until all chunks are merged into one contiguous Array.
    arr = tbl["a"].combine_chunks().to_numpy(zero_copy_only=True)
    print(arr)

def ray_data_arrow():
    import ray

    ray.init()

    table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    ds = ray.data.from_arrow(table)
    print(ds.take_all())

    # Work in Ray, then retrieve Arrow blocks
    arrow_refs = ds.to_arrow_refs()
    tables = ray.get(arrow_refs) # List[pyarrow.Table]
    for t in tables:
        print(t)


def arrow_cuda_buffer():
    import numpy as np
    import pyarrow.cuda as cuda
    # from pyarrow import cuda

    ctx = cuda.Context()
    arr = np.arange(10, dtype=np.int32)

    # Create a CUDA buffer from host data (this step transfers to device)
    cuda_buf = ctx.buffer_from_data(arr)
    print(cuda_buf)

    # But slicing the CUDA buffer is zero-copy on the device buffer
    cuda_slice = cuda_buf.slice(4 * 2, 4 * 5)  # ints 2..6 region (bytes-nased)
    print(cuda_slice)


if __name__ == "__main__":
    dataset = ds.dataset("data/events/", format="parquet")
    # dataset_scanner_compute(dataset)
    # arrow_tbl = duckdb_arrow_sql(dataset)
    # print(arrow_tbl)
    polars_lazy_scan()
    # arrow_to_pandas()
    # ray_data_arrow()
    # arrow_cuda_buffer()
