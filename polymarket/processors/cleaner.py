#!/usr/bin/env python3
"""
Polymarket data cleaning module.

Features:
1. clean_users: clean user trade data
   - Filter contract addresses and NaN prices
   - Split maker/taker into separate user records
   - Normalize to the YES-side perspective and direction
   - Sort by user and timestamp

2. clean_trades: clean trade data
   - Filter contract addresses and NaN prices
   - Normalize to the YES-side perspective
   - Preserve original fields
"""

import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# Contract addresses. Remove rows whose taker matches these addresses.
CONTRACT_ADDRESSES = {
    '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e',
    '0xc5d563a36ae78145c45a50134d48a1215220f80a'
}

# Default batch size.
DEFAULT_BATCH_SIZE = 5_000_000


def _process_users_batch(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Process a batch of user data.

    1. Filter NaN prices and contract addresses
    2. Split maker/taker
    3. Normalize to the YES-side perspective
    4. Normalize direction (SELL becomes BUY and token_amount becomes negative)
    """
    original_rows = len(df)

    # Add the original order index.
    df = df.reset_index(drop=True)
    df['_original_order'] = df.index

    # 1. Filter NaN prices.
    nan_mask = df['price'].isna()
    if nan_mask.any():
        df = df[~nan_mask]

    # 2. Filter contract addresses using lowercase comparison.
    contract_mask = df['taker'].str.lower().isin(CONTRACT_ADDRESSES)
    if contract_mask.any():
        df = df[~contract_mask]

    if len(df) == 0:
        return None

    # Common fields to retain.
    common_cols = ['timestamp', 'datetime', 'block_number', 'transaction_hash',
                   'event_id', 'market_id', 'condition_id', '_original_order',
                   'nonusdc_side', 'price', 'token_amount', 'usd_amount']

    # 3. Split both sides.
    maker_df = df[common_cols + ['maker', 'maker_direction']].copy()
    maker_df = maker_df.rename(columns={'maker': 'user', 'maker_direction': 'direction'})
    maker_df['role'] = 'maker'
    maker_df['_sub_order'] = 0

    taker_df = df[common_cols + ['taker', 'taker_direction']].copy()
    taker_df = taker_df.rename(columns={'taker': 'user', 'taker_direction': 'direction'})
    taker_df['role'] = 'taker'
    taker_df['_sub_order'] = 1

    result_df = pd.concat([maker_df, taker_df], ignore_index=True)

    # 4. Normalize to the YES-side perspective.
    is_token2 = result_df['nonusdc_side'] == 'token2'
    result_df.loc[is_token2, 'price'] = 1 - result_df.loc[is_token2, 'price']

    # 5. Normalize direction.
    is_sell = result_df['direction'] == 'SELL'
    result_df.loc[is_sell, 'token_amount'] = -result_df.loc[is_sell, 'token_amount']
    result_df['direction'] = 'BUY'

    # Preserve order.
    result_df = result_df.sort_values(['_original_order', '_sub_order'])
    result_df = result_df.drop(columns=['_original_order', '_sub_order', 'nonusdc_side'])

    # Final column order.
    result_df = result_df[['timestamp', 'datetime', 'block_number', 'transaction_hash',
                           'event_id', 'market_id', 'condition_id',
                           'user', 'role', 'price', 'token_amount', 'usd_amount']]

    return result_df


def _process_trades_batch(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Process a batch of trade data.

    1. Filter NaN prices and contract addresses
    2. Normalize to the YES-side perspective
    """
    # 1. Filter NaN prices.
    nan_mask = df['price'].isna()
    if nan_mask.any():
        df = df[~nan_mask]

    # 2. Filter contract addresses using lowercase comparison.
    contract_mask = df['taker'].str.lower().isin(CONTRACT_ADDRESSES)
    if contract_mask.any():
        df = df[~contract_mask]

    if len(df) == 0:
        return None

    # 3. Normalize to the YES-side perspective.
    df = df.copy()
    is_token2 = df['nonusdc_side'] == 'token2'
    df.loc[is_token2, 'price'] = 1 - df.loc[is_token2, 'price']
    df['nonusdc_side'] = 'token1'

    return df


def _sort_with_best_method(input_path: str, output_path: str, sort_columns: list):
    """Sort using the best available method."""
    import os
    temp_output = output_path + '.tmp'
    sorted_ok = False

    # Option 1: DuckDB.
    try:
        import duckdb
        logger.info("Sorting with DuckDB...")
        order_clause = ', '.join(sort_columns)
        conn = duckdb.connect()
        conn.execute(f"""
            COPY (
                SELECT * FROM read_parquet('{input_path}')
                ORDER BY {order_clause}
            ) TO '{temp_output}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
        conn.close()
        os.replace(temp_output, output_path)
        sorted_ok = True
    except ImportError:
        pass
    except Exception as e:
        logger.warning(f"DuckDB sort failed: {e}")

    # Option 2: Polars.
    if not sorted_ok:
        try:
            import polars as pl
            logger.info("Sorting with Polars...")
            df = pl.scan_parquet(input_path).sort(sort_columns).collect()
            df.write_parquet(temp_output, compression='snappy')
            os.replace(temp_output, output_path)
            sorted_ok = True
        except ImportError:
            pass
        except Exception as e:
            logger.warning(f"Polars sort failed: {e}")

    # Option 3: PyArrow.
    if not sorted_ok:
        logger.info("Sorting with PyArrow...")
        table = pq.read_table(input_path)
        sort_keys = [(col, 'ascending') for col in sort_columns]
        sorted_table = table.sort_by(sort_keys)
        pq.write_table(sorted_table, output_path, compression='snappy')


def clean_users(
    input_path: Path,
    output_path: Path,
    batch_size: int = DEFAULT_BATCH_SIZE,
    test_rows: Optional[int] = None
) -> dict:
    """
    Clean user trade data.

    Args:
        input_path: input `trades.parquet` path
        output_path: output `users.parquet` path
        batch_size: batch size
        test_rows: test mode, process only the first N rows

    Returns:
        Statistics dictionary
    """
    start_time = datetime.now()
    logger.info(f"Starting user data cleaning: {input_path}")

    if not input_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    parquet_file = pq.ParquetFile(input_path)
    total_rows = parquet_file.metadata.num_rows
    if test_rows:
        total_rows = min(total_rows, test_rows)

    logger.info(f"Total rows: {total_rows:,}")

    writer = None
    rows_processed = 0
    rows_written = 0
    batch_num = 0

    for batch in parquet_file.iter_batches(batch_size=batch_size):
        if test_rows and rows_processed >= test_rows:
            break

        batch_num += 1
        df = batch.to_pandas()

        if test_rows:
            remaining = test_rows - rows_processed
            df = df.head(remaining)

        result_df = _process_users_batch(df)

        if result_df is not None and len(result_df) > 0:
            result_table = pa.Table.from_pandas(result_df, preserve_index=False)

            if writer is None:
                writer = pq.ParquetWriter(str(output_path), result_table.schema, compression='snappy')

            writer.write_table(result_table)
            rows_written += len(result_df)

        rows_processed += len(df)
        progress = rows_processed / total_rows * 100
        logger.info(f"Batch {batch_num}: {rows_processed:,}/{total_rows:,} ({progress:.1f}%)")

    if writer:
        writer.close()

    clean_elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"Cleaning complete! Elapsed: {clean_elapsed:.1f}s")

    # Global sort.
    logger.info("Starting global sort (by user, timestamp)...")
    sort_start = datetime.now()
    _sort_with_best_method(str(output_path), str(output_path), ['user', 'timestamp'])
    sort_elapsed = (datetime.now() - sort_start).total_seconds()
    logger.info(f"Sorting complete! Elapsed: {sort_elapsed:.1f}s")

    total_elapsed = (datetime.now() - start_time).total_seconds()

    stats = {
        'input_rows': rows_processed,
        'output_rows': rows_written,
        'expansion_ratio': rows_written / rows_processed if rows_processed > 0 else 0,
        'elapsed_seconds': total_elapsed
    }

    logger.info(f"User data cleaning complete: {rows_processed:,} -> {rows_written:,} "
                f"({stats['expansion_ratio']:.2f}x), elapsed: {total_elapsed:.1f}s")

    return stats


def clean_trades(
    input_path: Path,
    output_path: Path,
    batch_size: int = DEFAULT_BATCH_SIZE,
    test_rows: Optional[int] = None
) -> dict:
    """
    Clean trade data.

    Args:
        input_path: input `trades.parquet` path
        output_path: output `quant.parquet` path
        batch_size: batch size
        test_rows: test mode, process only the first N rows

    Returns:
        Statistics dictionary
    """
    start_time = datetime.now()
    logger.info(f"Starting trade data cleaning: {input_path}")

    if not input_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    parquet_file = pq.ParquetFile(input_path)
    total_rows = parquet_file.metadata.num_rows
    if test_rows:
        total_rows = min(total_rows, test_rows)

    logger.info(f"Total rows: {total_rows:,}")

    writer = None
    rows_processed = 0
    rows_written = 0
    batch_num = 0

    for batch in parquet_file.iter_batches(batch_size=batch_size):
        if test_rows and rows_processed >= test_rows:
            break

        batch_num += 1
        df = batch.to_pandas()

        if test_rows:
            remaining = test_rows - rows_processed
            df = df.head(remaining)

        result_df = _process_trades_batch(df)

        if result_df is not None and len(result_df) > 0:
            result_table = pa.Table.from_pandas(result_df, preserve_index=False)

            if writer is None:
                writer = pq.ParquetWriter(str(output_path), result_table.schema, compression='snappy')

            writer.write_table(result_table)
            rows_written += len(result_df)

        rows_processed += len(df)
        progress = rows_processed / total_rows * 100
        logger.info(f"Batch {batch_num}: {rows_processed:,}/{total_rows:,} ({progress:.1f}%)")

    if writer:
        writer.close()

    elapsed = (datetime.now() - start_time).total_seconds()

    stats = {
        'input_rows': rows_processed,
        'output_rows': rows_written,
        'retention_ratio': rows_written / rows_processed if rows_processed > 0 else 0,
        'elapsed_seconds': elapsed
    }

    logger.info(f"Trade data cleaning complete: {rows_processed:,} -> {rows_written:,} "
                f"({stats['retention_ratio']*100:.1f}%), elapsed: {elapsed:.1f}s")

    return stats


def clean_trades_df(trades_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean a single trades DataFrame for incremental processing.

    Args:
        trades_df: original trades DataFrame

    Returns:
        Cleaned quant DataFrame
    """
    result = _process_trades_batch(trades_df)
    return result if result is not None else pd.DataFrame()


def clean_users_df(trades_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean a single trades DataFrame to generate users data for incremental processing.

    Args:
        trades_df: original trades DataFrame

    Returns:
        Cleaned users DataFrame
    """
    result = _process_users_batch(trades_df)
    return result if result is not None else pd.DataFrame()
