#!/usr/bin/env python3
"""
Parquet sorting script - memory-efficient version.
Uses DuckDB external sorting and is suitable for large files.

Usage:
    # Specify input and output file paths directly
    python scripts/sort_parquet.py users -i /path/to/users.parquet -o /path/to/users_sorted.parquet
    python scripts/sort_parquet.py quant -i /path/to/quant.parquet -o /path/to/quant_sorted.parquet

    # Use the default directory (data/data_clean)
    python scripts/sort_parquet.py users
    python scripts/sort_parquet.py quant

    # Test mode (process only the first 1 million rows)
    python scripts/sort_parquet.py users -i /path/to/users.parquet -o /path/to/test.parquet --test

cd /inspire/ssd/project/liyikang/wangzhengjie-253108090056/poly_onchain

nohup python scripts/sort_parquet.py users \
    -i /inspire/hdd/project/liyikang/public/Polymarket_dataset/dataset_latest/users.parquet \
    -o /inspire/hdd/project/liyikang/public/Polymarket_dataset/dataset_latest/users_sorted.parquet \
    > logs/sort_users.log 2>&1 &

nohup python scripts/sort_parquet.py quant \
    -i /inspire/hdd/project/liyikang/public/Polymarket_dataset/dataset_latest/quant.parquet \
    -o /inspire/hdd/project/liyikang/public/Polymarket_dataset/dataset_latest/quant_sorted.parquet \
    > logs/sort_quant.log 2>&1 &

# View progress
tail -f logs/sort_users.log

Arguments:
    users/quant   file type to sort
    -i/--input    input file path (full path)
    -o/--output   output file path (full path)
    --test        test mode, process only the first 1 million rows
"""

import argparse
import os
import sys
import time
import gc
import shutil
from pathlib import Path

# DuckDB is used for memory-efficient external sorting.
import duckdb


def log(msg: str):
    """Timestamped log output with immediate flush."""
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def get_temp_dir() -> str:
    """Get the temporary directory: SSD on the cluster, project directory locally."""
    if os.path.exists('/inspire/ssd'):
        # Cluster: use the SSD directory to avoid HDD I/O issues.
        temp_dir = '/inspire/ssd/project/liyikang/wangzhengjie-253108090056/.duckdb_temp'
    else:
        # Local: use the project directory.
        script_dir = os.path.dirname(os.path.abspath(__file__))
        temp_dir = os.path.join(script_dir, '..', '.duckdb_temp')
    temp_dir = os.path.abspath(temp_dir)
    os.makedirs(temp_dir, exist_ok=True)
    return temp_dir


def cleanup_temp(temp_dir: str):
    """Clean up DuckDB temporary files."""
    if os.path.exists(temp_dir):
        try:
            shutil.rmtree(temp_dir)
        except Exception as e:
            log(f"Failed to clean temporary directory: {e}")


def get_memory_limit_gb():
    """Use half of available memory as the limit."""
    try:
        import psutil
        available_gb = psutil.virtual_memory().available / (1024**3)
        # Use 50% of available memory, up to 64GB.
        return min(available_gb * 0.5, 64)
    except ImportError:
        return 8  # Default: 8GB


def sort_users_parquet(input_path: str, output_path: str, test_mode: bool = False):
    """
    Sort `users.parquet`.
    Sort priority: 1. user, 2. timestamp
    """
    log("=" * 60)
    log("Sorting users.parquet")
    log(f"Input: {input_path}")
    log(f"Output: {output_path}")
    log(f"Test mode: {test_mode}")
    log("=" * 60)

    mem_limit = get_memory_limit_gb()
    log(f"Memory limit: {mem_limit:.1f} GB")

    start_time = time.time()

    # Get temporary directory.
    temp_dir = get_temp_dir()
    log(f"Temporary directory: {temp_dir}")

    # Create DuckDB connection and configure external sorting.
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{mem_limit:.0f}GB'")
    con.execute(f"SET temp_directory = '{temp_dir}'")
    con.execute("SET preserve_insertion_order = false")  # Allow parallel processing.

    try:
        # Build query.
        if test_mode:
            # In test mode, limit rows first and then sort.
            query = f"""
                COPY (
                    SELECT * FROM (
                        SELECT *
                        FROM read_parquet('{input_path}')
                        LIMIT 1000000
                    )
                    ORDER BY user ASC, CAST(timestamp AS BIGINT) ASC
                ) TO '{output_path}'
                (FORMAT PARQUET, COMPRESSION 'zstd', ROW_GROUP_SIZE 100000)
            """
        else:
            query = f"""
                COPY (
                    SELECT *
                    FROM read_parquet('{input_path}')
                    ORDER BY user ASC, CAST(timestamp AS BIGINT) ASC
                ) TO '{output_path}'
                (FORMAT PARQUET, COMPRESSION 'zstd', ROW_GROUP_SIZE 100000)
            """

        log("Starting sort...")
        con.execute(query)

        elapsed = time.time() - start_time

        # Validate output.
        result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()
        log(f"Done! Output rows: {result[0]:,}")
        log(f"Elapsed: {elapsed:.1f} s")

        # Show sorted sample rows.
        log("First 5 rows after sorting:")
        samples = con.execute(f"""
            SELECT user, timestamp
            FROM read_parquet('{output_path}')
            LIMIT 5
        """).fetchall()
        for user, ts in samples:
            print(f"  {user[:20]}... | {ts}", flush=True)

    finally:
        con.close()
        del con
        gc.collect()
        cleanup_temp(temp_dir)
        log("Memory released")


def sort_quant_parquet(input_path: str, output_path: str, test_mode: bool = False):
    """
    Sort `quant.parquet`.
    Sort priority: 1. event_id, 2. market_id, 3. timestamp
    """
    log("=" * 60)
    log("Sorting quant.parquet")
    log(f"Input: {input_path}")
    log(f"Output: {output_path}")
    log(f"Test mode: {test_mode}")
    log("=" * 60)

    mem_limit = get_memory_limit_gb()
    log(f"Memory limit: {mem_limit:.1f} GB")

    start_time = time.time()

    # Get temporary directory.
    temp_dir = get_temp_dir()
    log(f"Temporary directory: {temp_dir}")

    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{mem_limit:.0f}GB'")
    con.execute(f"SET temp_directory = '{temp_dir}'")
    con.execute("SET preserve_insertion_order = false")

    try:
        # Use TRY_CAST to handle null or invalid values.
        if test_mode:
            query = f"""
                COPY (
                    SELECT * FROM (
                        SELECT *
                        FROM read_parquet('{input_path}')
                        LIMIT 1000000
                    )
                    ORDER BY
                        COALESCE(TRY_CAST(event_id AS BIGINT), 0) ASC,
                        COALESCE(TRY_CAST(market_id AS BIGINT), 0) ASC,
                        COALESCE(TRY_CAST(timestamp AS BIGINT), 0) ASC
                ) TO '{output_path}'
                (FORMAT PARQUET, COMPRESSION 'zstd', ROW_GROUP_SIZE 100000)
            """
        else:
            query = f"""
                COPY (
                    SELECT *
                    FROM read_parquet('{input_path}')
                    ORDER BY
                        COALESCE(TRY_CAST(event_id AS BIGINT), 0) ASC,
                        COALESCE(TRY_CAST(market_id AS BIGINT), 0) ASC,
                        COALESCE(TRY_CAST(timestamp AS BIGINT), 0) ASC
                ) TO '{output_path}'
                (FORMAT PARQUET, COMPRESSION 'zstd', ROW_GROUP_SIZE 100000)
            """

        log("Starting sort...")
        con.execute(query)

        elapsed = time.time() - start_time

        result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()
        log(f"Done! Output rows: {result[0]:,}")
        log(f"Elapsed: {elapsed:.1f} s")

        log("First 5 rows after sorting:")
        samples = con.execute(f"""
            SELECT event_id, market_id, timestamp, event_title
            FROM read_parquet('{output_path}')
            LIMIT 5
        """).fetchall()
        for eid, mid, ts, title in samples:
            print(f"  event={eid}, market={mid}, ts={ts} | {title[:40]}...", flush=True)

    finally:
        con.close()
        del con
        gc.collect()
        cleanup_temp(temp_dir)
        log("Memory released")


def main():
    parser = argparse.ArgumentParser(description='Parquet sorting script')
    parser.add_argument('target', choices=['users', 'quant'],
                        help='File type to sort: users or quant')
    parser.add_argument('-i', '--input', default=None,
                        help='Input file path (full path)')
    parser.add_argument('-o', '--output', default=None,
                        help='Output file path (full path)')
    parser.add_argument('--test', action='store_true',
                        help='Test mode, process only the first 1 million rows')

    args = parser.parse_args()

    # Default paths.
    default_dir = Path('data/data_clean')
    suffix = "_test" if args.test else "_sorted"

    if args.target == 'users':
        input_file = args.input or str(default_dir / 'users.parquet')
        output_file = args.output or str(default_dir / f'users{suffix}.parquet')

        if not os.path.exists(input_file):
            print(f"Error: file does not exist {input_file}")
            sys.exit(1)

        sort_users_parquet(input_file, output_file, args.test)

    elif args.target == 'quant':
        input_file = args.input or str(default_dir / 'quant.parquet')
        output_file = args.output or str(default_dir / f'quant{suffix}.parquet')

        if not os.path.exists(input_file):
            print(f"Error: file does not exist {input_file}")
            sys.exit(1)

        sort_quant_parquet(input_file, output_file, args.test)

    print("\n" + "="*60, flush=True)
    print("All done!", flush=True)
    print("="*60, flush=True)


if __name__ == '__main__':
    main()
