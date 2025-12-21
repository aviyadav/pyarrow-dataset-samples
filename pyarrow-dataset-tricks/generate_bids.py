import polars as pl
import numpy as np
import uuid
from datetime import datetime, timedelta
import os
from benchmark import measure_performance


@measure_performance
def generate_bids(n_rows=1000000):
    print(f"Generating {n_rows} rows of data using Polars...")
    
    # 1. auction_id: UUIDs
    # Generating UUIDs
    auction_ids = [str(uuid.uuid4()) for _ in range(n_rows)]
    
    # 2. country_code (2 letter)
    countries = ['US', 'UK', 'IN', 'CA', 'DE', 'FR', 'JP', 'AU', 'BR', 'MX']
    country_codes = np.random.choice(countries, n_rows)
    
    # 3. price
    prices = np.random.uniform(0.10, 1000.00, n_rows).round(2)
    
    # 4. ts (timestamp) & 5. date_started
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    start_ts = start_date.timestamp()
    end_ts = end_date.timestamp()
    
    random_ts = np.random.uniform(start_ts, end_ts, n_rows)
    # Convert using polars functionality or just numpy -> datetime64
    timestamps = (random_ts * 1000000).astype('datetime64[us]') # Polars likes microseconds usually or ns
    
    # 6. user_id
    user_ids = np.random.randint(1000, 9999999, n_rows)
    
    df = pl.DataFrame({
        'auction_id': auction_ids,
        'country': country_codes,
        'price': prices,
        'ts': timestamps,
        'user_id': user_ids
    })
    
    # Add date_started derived from ts
    df = df.with_columns(
        pl.col('ts').dt.date().cast(pl.Utf8).alias('ds')
    )
    
    output_dir = "analytics/bids"
    print(f"Saving to {output_dir}...")
    
    # Polars writes partitioned datasets
    # Using pyarrow options for robust partitioning
    df.write_parquet(
        output_dir,
        use_pyarrow=True,
        pyarrow_options={"partition_cols": ['country', 'ds']}
    )
    print("Done!")

if __name__ == "__main__":
    generate_bids(1000000)
