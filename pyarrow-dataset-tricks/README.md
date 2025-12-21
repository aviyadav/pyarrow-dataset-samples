# PyArrow Dataset Tricks

This project demonstrates various techniques for efficient data processing using the PyArrow Dataset API. It includes examples of projection pushdown, predicate pushdown, partition pruning, and optimizing storage layout.

## Setup

1.  **Install Dependencies**:
    Ensure you have `pyarrow`, `pandas`, `numpy`, and `polars` installed.
    ```bash
    pip install pyarrow pandas numpy polars
    ```

2.  **Generate Data**:
    Run the generation script to create a synthetic Hive-partitioned dataset.
    ```bash
    python generate_bids.py
    ```
    This will create ~1 million rows of bid data in `analytics/bids`, partitioned by `country` and `date_started` (`ds`).

## Features & Examples

The `main.py` script contains several functions showcasing different PyArrow capabilities:

-   **`projection_pushdown_example`**:
    Demonstrates reading only a subset of columns (`auction_id`, `country`, `price`, `ts`). This minimizes I/O by not reading unused columns.

-   **`predicate_pushdown_example`**:
    Applies filters (e.g., `country == 'AU'` AND `price > 100`) during the scan. PyArrow uses these predicates to skip non-matching row groups.

-   **`partition_pruning`**:
    Filters based on partition columns (e.g., `country` directory names). This allows PyArrow to completely skip reading files in irrelevant partitions.

-   **`row_group_layout`**:
    Shows how to rewrite a dataset with a specific partitioning scheme (`country`, `ds`) and control file compaction/row group sizes options (`max_rows_per_file`, `max_rows_per_group`).

-   **`batch_stream`**:
    Demonstrates iterating over the dataset in batches involving a filter. This is useful for processing datasets that are larger than memory.

## Usage

To run an example, uncomment the desired function call in the `main` block of `main.py`:

```python
def main(dataset):
    # projection_pushdown_example(dataset)
    # predicate_pushdown_example(dataset)
    # partition_pruning(dataset)
    batch_stream(dataset)
```

Then execute the script:

```bash
python main.py
```
