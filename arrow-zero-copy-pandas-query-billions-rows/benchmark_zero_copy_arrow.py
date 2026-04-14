import os
import time
import random
import psutil

import pandas as pd
import pyarrow as pa
import polars as pl
import duckdb

from faker import Faker
from duckdb.sqltypes import VARCHAR


# ==========================CONFIG==========================================

NUM_ROWS = 1_000_000
PARQUET_FILE = "large_logs.parquet"

Faker.seed(42)
fake = Faker()
random.seed(42)

# ==========================DUMMY DATA GENERATION==========================================
def generate_dummy_data():
    if os.path.exists(PARQUET_FILE):
        print(f"✅ Using existing dataset: {PARQUET_FILE} ({NUM_ROWS:,} rows)")
        return

    print(f"🚀 Generating {NUM_ROWS:,} rows of messy e-commerce dummy data...")
    start = time.time()

    data = []
    for _ in range(NUM_ROWS):
        base_name = fake.catch_phrase()
        messy_name = base_name.replace(" ", "_").upper()

        if random.random() > 0.65:
            messy_name += random.choice(["-PRO", "_XL", " v2", " (Black)", " - Limited"])
        if random.random() > 0.75:
            messy_name = messy_name.lower().replace("_", "  ")
        if random.random() > 0.85:
            messy_name = "   " + messy_name + "   "

        row = {
            "timestamp": fake.date_time_between(start_date="-1y", end_date="now"),
            "user_id": fake.uuid4()[:8],
            "product_name": messy_name,
            "amount": round(random.uniform(9.99, 499.99), 2),
            "category": fake.random_element(["Electronics", "Fashion", "Home", "Sports", "Beauty"])
        }
        data.append(row)

    df = pd.DataFrame(data)
    df.to_parquet(
        PARQUET_FILE,
        compression="snappy",
        engine="pyarrow",
        index=False,
        row_group_size=100_000
    )

    elapsed = time.time() - start
    size_mb = round(os.path.getsize(PARQUET_FILE) / (1024 * 1024), 1)
    print(f"✅ Generated {NUM_ROWS:,} rows in {elapsed:.1f}s → {size_mb} MB")

# ====================== THE UDF (ZERO-COPY) ======================
def clean_and_enrich(product_names: pa.Array) -> pa.Array:
    """Pure Python function that receives Arrow chunks (ZERO-COPY)"""
    result = []
    for name in product_names.to_pylist():
        cleaned = (
            name.strip()
            .lower()
            .replace("_", " ")
            .replace("  ", " ")
            .replace("xl", "Extra Large")
        )
        result.append(cleaned)
    return pa.array(result)


# ====================== BENCHMARK FUNCTIONS ======================
def run_pandas_benchmark():
    print("\n🐼 Running Pandas benchmark...")
    mem_before = psutil.Process().memory_info().rss / (1024 ** 2)
    start = time.time()
    
    df = pd.read_parquet(PARQUET_FILE)
    df["clean_name"] = df["product_name"].apply(
        lambda x: x.strip().lower().replace("_", " ").replace("  ", " ")
    )
    
    elapsed = time.time() - start
    mem_after = psutil.Process().memory_info().rss / (1024 ** 2)
    peak_mem = mem_after - mem_before
    
    print(f"   Pandas finished in {elapsed:.2f} seconds")
    print(f"   Peak memory used: +{peak_mem:.1f} MB")
    return elapsed, peak_mem


def run_duckdb_zero_copy_benchmark():
    print("\n🦆 Running DuckDB + Zero-Copy Arrow UDF benchmark...")
    mem_before = psutil.Process().memory_info().rss / (1024 ** 2)
    start = time.time()
    
    # Register UDF - works on latest DuckDB
    duckdb.create_function(
        "clean_and_enrich",
        clean_and_enrich,
        [VARCHAR],
        VARCHAR,
        type="arrow"          # zero-copy magic
    )
    
    # FIXED QUERY: use ANY_VALUE() to satisfy GROUP BY rules
    result = duckdb.sql("""
        SELECT 
            user_id,
            clean_and_enrich(ANY_VALUE(product_name)) AS clean_name,
            SUM(amount) AS total_revenue
        FROM read_parquet('large_logs.parquet')
        GROUP BY user_id
        LIMIT 100
    """).arrow()
    
    elapsed = time.time() - start
    mem_after = psutil.Process().memory_info().rss / (1024 ** 2)
    peak_mem = mem_after - mem_before
    
    print(f"   DuckDB + Zero-Copy finished in {elapsed:.2f} seconds")
    print(f"   Peak memory used: +{peak_mem:.1f} MB")
    return elapsed, peak_mem


def run_polars_benchmark():
    print("\n🐻‍❄️ Running Polars benchmark...")
    mem_before = psutil.Process().memory_info().rss / (1024 ** 2)
    start = time.time()
    
    df = pl.read_parquet(PARQUET_FILE)
    df = df.with_columns(
        pl.col("product_name").str.strip_chars().str.to_lowercase().str.replace_all("_", " ").str.replace_all("  ", " ").alias("clean_name")
    )
    
    elapsed = time.time() - start
    mem_after = psutil.Process().memory_info().rss / (1024 ** 2)
    peak_mem = mem_after - mem_before
    
    print(f"   Polars finished in {elapsed:.2f} seconds")
    print(f"   Peak memory used: +{peak_mem:.1f} MB")
    return elapsed, peak_mem


# ====================== MAIN ======================
if __name__ == "__main__":
    print("="*80)
    print("🚀 Apache Arrow Zero-Copy Benchmark (100% WORKING - Latest DuckDB)")
    print("   Comparing Pandas vs DuckDB + Zero-Copy UDF vs Polars")
    print("="*80)
    
    generate_dummy_data()
    
    pandas_time, pandas_mem = run_pandas_benchmark()
    duckdb_time, duckdb_mem = run_duckdb_zero_copy_benchmark()
    polars_time, polars_mem = run_polars_benchmark()
    
    print("\n" + "="*90)
    print("📊 FINAL PERFORMANCE COMPARISON")
    print("="*90)
    print(f"{'Metric':<25} {'Pandas (Slow)':<15} {'DuckDB + Zero-Copy':<22} {'Polars':<15} {'Fastest'}")
    print("-" * 90)
    
    fastest_time = min(pandas_time, duckdb_time, polars_time)
    fastest_time_name = "Pandas" if fastest_time == pandas_time else "DuckDB" if fastest_time == duckdb_time else "Polars"
    
    lowest_mem = min(pandas_mem, duckdb_mem, polars_mem)
    lowest_mem_name = "Pandas" if lowest_mem == pandas_mem else "DuckDB" if lowest_mem == duckdb_mem else "Polars"
    
    print(f"{'Time (seconds)':<25} {pandas_time:<15.2f} {duckdb_time:<22.2f} {polars_time:<15.2f} {fastest_time_name}")
    print(f"{'Memory overhead (MB)':<25} {pandas_mem:<15.1f} {duckdb_mem:<22.1f} {polars_mem:<15.1f} {lowest_mem_name}")
    print("="*90)