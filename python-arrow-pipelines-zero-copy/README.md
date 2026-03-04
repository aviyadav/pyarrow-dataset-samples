# PyArrow Zero-Copy Pipelines

Demonstration of zero-copy data processing patterns with PyArrow, showcasing efficient data pipelines across multiple tools in the Python data ecosystem.

## Overview

This project illustrates how to build high-performance data pipelines by leveraging PyArrow's zero-copy memory management capabilities. It demonstrates interoperability between PyArrow and popular data processing libraries including DuckDB, Polars, Pandas, and Ray Data—all while minimizing memory copies and maximizing performance.

## Features

The project includes multiple example workflows:

### 1. **PyArrow Dataset Scanner + Compute**
- Pushdown projections and filters to the scan layer
- Columnar compute on Arrow batches
- Write partitioned Parquet datasets
- Cross-platform timestamp handling (Windows-compatible)

### 2. **DuckDB + Arrow Integration**
- Register PyArrow datasets directly in DuckDB
- Execute SQL queries with zero-copy data access
- Return results as Arrow tables

### 3. **Polars Lazy Evaluation**
- Lazy scan of Parquet files
- Zero-copy conversion between Polars and Arrow
- Bidirectional Arrow ↔ Polars dataframe exchange

### 4. **Arrow to Pandas**
- Arrow-native dtype preservation in Pandas
- Zero-copy to NumPy (when contiguous)
- Type mapping with `ArrowDtype`

### 5. **Ray Data Integration** *(Linux/macOS only)*
- Distributed processing with Ray Data
- Arrow table to Ray dataset conversion
- Retrieve Arrow blocks from distributed computations

### 6. **Arrow CUDA Buffers**
- GPU buffer management
- Zero-copy slicing on device memory

## Project Structure

```
.
├── main.py                 # Example pipelines demonstrating zero-copy patterns
├── generate_events.py      # Parallel event data generator using multiprocessing
├── pyproject.toml          # Project dependencies
└── data/
    └── events/             # Generated Parquet files (500 files × 10K rows)
```

## Setup

### Prerequisites
- Python 3.13+
- [uv](https://github.com/astral-sh/uv) package manager

### Installation

1. Clone or navigate to the project directory:
```bash
cd python-arrow-pipelines-zero-copy
```

2. Install dependencies:
```bash
uv sync
```

3. Activate the virtual environment:
```bash
source .venv/bin/activate
```

## Usage

### Generate Sample Data

First, generate the sample event dataset:

```bash
python generate_events.py
```

This creates 500 Parquet files in `data/events/` with ~10,000 rows each (5 million total rows), using multiprocessing for efficient generation.

### Run Examples

Edit [main.py](main.py) to uncomment the example you want to run, then:

```bash
python main.py
```

**Available examples:**
- `dataset_scanner_compute(dataset)` - PyArrow dataset processing with filters
- `duckdb_arrow_sql(dataset)` - SQL queries via DuckDB
- `polars_lazy_scan()` - Polars lazy evaluation
- `arrow_to_pandas()` - Arrow to Pandas conversion
- `ray_data_arrow()` - Ray Data distributed processing (Linux/macOS only)
- `arrow_cuda_buffer()` - CUDA buffer operations (requires CUDA-enabled PyArrow)

## Platform Notes

### Windows Compatibility

- **Timezone handling**: The project uses the `tzdata` package and sets `ARROW_TZDATA_DIR` to ensure consistent timezone support on Windows
- **Ray Data**: Ray is not installed on Windows (specified via `sys_platform != 'win32'` in dependencies). The `ray_data_arrow()` example will not work on Windows

### Linux/macOS

All examples are fully supported.

## Dependencies

- **pyarrow** ≥23.0.1 - Core Arrow library
- **duckdb** ≥1.4.4 - SQL query engine
- **polars** ≥1.38.1 - DataFrame library
- **pandas** ≥3.0.1 - Data analysis library
- **ray** ≥2.54.0 - Distributed computing (Linux/macOS only)
- **tzdata** ≥2025.3 - Timezone database
- **typing-extensions** ≥4.0.0 - Required by Ray

## Performance Tips

1. **Minimize copies**: Use `zero_copy_only=True` when converting to NumPy to ensure strict zero-copy behavior
2. **Batch processing**: Process data in batches to control memory usage
3. **Pushdown filters**: Apply filters at the scan layer to reduce data movement
4. **Columnar operations**: Use PyArrow compute functions to operate on entire columns
5. **Chunked arrays**: Call `combine_chunks()` before zero-copy operations that require contiguous memory

## Key Concepts

### Zero-Copy Operations

Zero-copy operations share memory buffers between data structures rather than creating duplicates. This is especially important for:
- Large datasets
- High-frequency data exchanges between libraries
- Memory-constrained environments

### Apache Arrow Format

Arrow provides a language-agnostic columnar memory format that enables:
- Efficient analytical operations
- Fast serialization/deserialization
- Zero-copy data sharing across processes and languages

## License

This is a sample/demonstration project.
