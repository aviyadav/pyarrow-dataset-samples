import math
import multiprocessing as mp
import os
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Generator

import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
NUM_FILES = 500
ROWS_PER_FILE = 10_000
OUTPUT_DIR = "data/events"

# How many rows to build in a single tight loop before flushing to a
# ParquetWriter.  Keeping this small caps per-worker peak RAM.
BATCH_SIZE = 2_000

# Leave 2 cores free for the OS and the main process.
MAX_WORKERS = max(1, (os.cpu_count() or 4) - 2)

# How many files can be in-flight across all workers at once.
# Each in-flight file holds at most BATCH_SIZE rows in RAM at a time, so
# total peak RAM ≈ MAX_CONCURRENT_FILES * BATCH_SIZE * ~500 B/row ≈ safe.
MAX_CONCURRENT_FILES = MAX_WORKERS * 2

SEED = 999

# ---------------------------------------------------------------------------
# Reference data (module-level so worker processes inherit it cheaply via
# copy-on-write after fork on Linux; on Windows they are re-created once
# per worker inside the initializer, which is still cheap).
# ---------------------------------------------------------------------------
USER_IDS = [f"user_{i:04d}" for i in range(1, 1501)]
EVENT_NAMES = [
    "purchase",
    "refund",
    "signup",
    "login",
    "logout",
    "add_to_cart",
    "remove_from_cart",
    "checkout",
    "subscription_start",
    "subscription_cancel",
]
CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "INR", "CNY", "KRW"]

SCHEMA = pa.schema(
    [
        pa.field("user_id", pa.string()),
        pa.field("event_id", pa.string()),
        pa.field("event_name", pa.string()),
        pa.field("amount", pa.float64()),
        pa.field("currency", pa.string()),
        pa.field("ts", pa.timestamp("us", tz="UTC")),
    ]
)

# ---------------------------------------------------------------------------
# Per-worker initializer
# ---------------------------------------------------------------------------


def _worker_init(seed_base: int) -> None:
    """Seed each worker's RNG with a unique value derived from its PID."""
    random.seed(seed_base ^ os.getpid())


# ---------------------------------------------------------------------------
# Row-level helpers  (pure functions → safe for multiprocessing)
# ---------------------------------------------------------------------------

_BASE_TS = datetime.now(tz=timezone.utc) - timedelta(days=30)
_WINDOW_SECONDS = 30 * 24 * 3600


def _random_amount(event_name: str) -> float:
    if event_name in ("signup", "login", "logout", "add_to_cart", "remove_from_cart"):
        return 0.0
    if event_name == "refund":
        return round(random.uniform(-500.0, -0.01), 2)
    return round(random.uniform(0.99, 999.99), 2)


def _iter_batches(
    file_index: int,
    rows_per_file: int,
    batch_size: int,
) -> Generator[pa.RecordBatch, None, None]:
    """
    Yield Arrow RecordBatches that together make up one full file.
    Only `batch_size` rows are held in RAM at any moment.
    """
    event_id_base = file_index * rows_per_file + 1
    rows_remaining = rows_per_file

    while rows_remaining > 0:
        n = min(batch_size, rows_remaining)

        user_ids: list[str] = []
        event_ids: list[str] = []
        event_names: list[str] = []
        amounts: list[float] = []
        currencies: list[str] = []
        timestamps: list[datetime] = []

        row_start = event_id_base + (rows_per_file - rows_remaining)
        for i in range(n):
            ename = random.choice(EVENT_NAMES)
            user_ids.append(random.choice(USER_IDS))
            event_ids.append(f"evt_{row_start + i:08d}")
            event_names.append(ename)
            amounts.append(_random_amount(ename))
            currencies.append(random.choice(CURRENCIES))
            timestamps.append(
                _BASE_TS + timedelta(seconds=random.randint(0, _WINDOW_SECONDS))
            )

        batch = pa.record_batch(
            {
                "user_id": pa.array(user_ids, type=pa.string()),
                "event_id": pa.array(event_ids, type=pa.string()),
                "event_name": pa.array(event_names, type=pa.string()),
                "amount": pa.array(amounts, type=pa.float64()),
                "currency": pa.array(currencies, type=pa.string()),
                "ts": pa.array(timestamps, type=pa.timestamp("us", tz="UTC")),
            },
            schema=SCHEMA,
        )

        yield batch
        rows_remaining -= n


# ---------------------------------------------------------------------------
# Worker task
# ---------------------------------------------------------------------------


def _write_file(args: tuple[int, int, int, str]) -> tuple[int, str, float]:
    """
    Generate and write one Parquet file entirely within a worker process.
    Returns (file_index, output_path, elapsed_seconds).
    """
    file_index, rows_per_file, batch_size, output_dir = args
    output_path = os.path.join(output_dir, f"events_part_{file_index:05d}.parquet")

    t0 = time.perf_counter()

    writer = pq.ParquetWriter(output_path, SCHEMA, compression="snappy")
    try:
        for batch in _iter_batches(file_index, rows_per_file, batch_size):
            writer.write_batch(batch)
    finally:
        writer.close()

    elapsed = time.perf_counter() - t0
    return file_index, output_path, elapsed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    total_rows = NUM_FILES * ROWS_PER_FILE
    num_batches_per_file = math.ceil(ROWS_PER_FILE / BATCH_SIZE)

    print(f"{'=' * 60}")
    print(f"  Files to generate : {NUM_FILES}")
    print(f"  Rows per file     : {ROWS_PER_FILE:,}")
    print(
        f"  Batch size        : {BATCH_SIZE:,}  ({num_batches_per_file} batch(es)/file)"
    )
    print(f"  Total rows        : {total_rows:,}")
    print(f"  Workers           : {MAX_WORKERS}  (of {os.cpu_count()} CPUs)")
    print(f"  Max in-flight     : {MAX_CONCURRENT_FILES} files")
    print(f"  Output dir        : {OUTPUT_DIR}/")
    print(f"{'=' * 60}\n")

    # Build task arguments for every file upfront (just ints + str → tiny).
    tasks = [
        (file_index, ROWS_PER_FILE, BATCH_SIZE, OUTPUT_DIR)
        for file_index in range(NUM_FILES)
    ]

    t_start = time.perf_counter()
    completed = 0

    # `maxtasksperchild` recycles workers periodically to prevent any
    # slow memory creep in long runs.
    with mp.Pool(
        processes=MAX_WORKERS,
        initializer=_worker_init,
        initargs=(SEED,),
        maxtasksperchild=20,
    ) as pool:
        # imap_unordered + chunksize=1 means:
        #   • results stream back as soon as each file is done (low latency)
        #   • at most MAX_CONCURRENT_FILES tasks are submitted at once
        #     (backpressure → caps total in-flight RAM)
        it = pool.imap_unordered(_write_file, tasks, chunksize=1)

        for file_index, path, elapsed in it:
            completed += 1
            pct = completed / NUM_FILES * 100
            print(
                f"  [{completed:>4}/{NUM_FILES}] {pct:5.1f}%  {path}  ({elapsed:.2f}s)"
            )

    wall = time.perf_counter() - t_start
    throughput = total_rows / wall

    print(f"\n{'=' * 60}")
    print(f"  Done in {wall:.2f}s")
    print(f"  Throughput: {throughput:,.0f} rows/s")
    print(f"  {total_rows:,} rows across {NUM_FILES} files in '{OUTPUT_DIR}/'")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    # Required on Windows / macOS (spawn start method).
    mp.freeze_support()
    main()
