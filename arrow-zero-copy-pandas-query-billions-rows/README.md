# Arrow Zero-Copy Benchmarks 🚀

This project benchmarks the performance of data processing across different libraries: **Pandas**, **DuckDB (with Zero-Copy Arrow UDFs)**, and **Polars**.

The goal is to demonstrate the speed of DuckDB's zero-copy read from Parquet combined with Apache Arrow, compared to other popular tools.

## The Benchmark

The benchmark involves:
1. Generating a dataset with 1,000,000 rows of messy e-commerce logs (saved as `large_logs.parquet`).
2. Performing string cleaning (lowercasing, replacing characters, stripping spaces) on the `product_name` column.

### Libraries Evaluated
- **Pandas**: Traditional in-memory processing.
- **Polars**: Modern, fast DataFrame library built on Rust, utilizing Apache Arrow arrays.
- **DuckDB**: Fast analytical database process utilizing zero-copy Apache Arrow UDFs for pure Python string manipulation over columns.

## Results

DuckDB utilizing Zero-Copy Arrow UDFs is generally the fastest, avoiding any data serialization overhead when moving between DuckDB execution and Python string functions. Polars natively performs very fast optimizations, outperforming Pandas significantly.

| Metric | Pandas (Slow) | DuckDB + Zero-Copy | Polars | Fastest |
|--------|---------------|--------------------|--------|---------|
| Time (seconds)         | ~ 0.88s       | ~ 0.15s            | ~ 0.35s| **DuckDB** |
| Memory overhead (MB)   | ~ 198.5 MB    | ~ 204.2 MB         | ~ 240.4 MB | **Pandas** |

*(Results based on a typical consumer CPU running the generated dataset)*

## Setup & Running

This project uses `uv` for modern Python package management and resolving dependencies.

```bash
# Run the benchmark directly (uv will handle the dependencies)
uv run python3 benchmark_zero_copy_arrow.py
```

### Dependencies
Required dependencies are listed in `pyproject.toml` and include:
- `duckdb` (>=1.5.2)
- `pandas`
- `polars`
- `pyarrow`
- `faker` (for dummy data generation)
- `psutil` (for memory telemetry)

## Important Notes on DuckDB and Types
For DuckDB version 1.5.2+, the typing constraints for the UDFs have moved correctly to the `duckdb.sqltypes` module. E.g.:
```python
from duckdb.sqltypes import VARCHAR
```
Using the legacy `duckdb.typing` will result in a `ModuleNotFoundError`.
