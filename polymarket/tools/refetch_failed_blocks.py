#!/usr/bin/env python3
"""
Script for refetching failed block ranges.

Reads failed block ranges from a failed_blocks file, refetches them one by one,
and saves them into separate parquet files.
"""
import os
import sys
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Add the project root to the path.
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Imports are now available.
from polymarket.fetchers.rpc import LogFetcher
from polymarket.processors import (
    EventDecoder,
    extract_trades,
    load_token_mapping,
    clean_trades_df,
    clean_users_df
)
from polymarket.config import MARKETS_FILE, MISSING_MARKETS_FILE
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def read_failed_blocks(failed_blocks_file):
    """Read the list of failed block ranges."""
    blocks = []
    with open(failed_blocks_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and '-' in line:
                start, end = line.split('-')
                blocks.append((int(start), int(end)))
    return blocks


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/refetch_failed_blocks.py <failed_blocks_file>")
        print("Example: python scripts/refetch_failed_blocks.py data/failed_blocks_20251230_055516.txt")
        sys.exit(1)

    failed_blocks_file = Path(sys.argv[1])
    if not failed_blocks_file.exists():
        logger.error(f"File does not exist: {failed_blocks_file}")
        sys.exit(1)

    # Read failed block ranges.
    failed_ranges = read_failed_blocks(failed_blocks_file)
    logger.info(f"Read {len(failed_ranges)} failed block ranges")

    # Initialize.
    fetcher = LogFetcher()
    decoder = EventDecoder()

    # Load token mappings.
    token_mapping = load_token_mapping(MARKETS_FILE)
    if MISSING_MARKETS_FILE.exists():
        token_mapping.update(load_token_mapping(MISSING_MARKETS_FILE))
    logger.info(f"Loaded {len(token_mapping)} token mappings")

    # Prepare output files.
    output_dir = project_root / 'data' / 'dataset'
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = failed_blocks_file.stem.replace('failed_blocks_', '')
    orderfilled_file = output_dir / f'orderfilled_refetched_{timestamp}.parquet'
    trades_file = output_dir / f'trades_refetched_{timestamp}.parquet'

    data_clean_dir = project_root / 'data' / 'data_clean'
    data_clean_dir.mkdir(parents=True, exist_ok=True)
    quant_file = data_clean_dir / f'quant_refetched_{timestamp}.parquet'
    users_file = data_clean_dir / f'users_refetched_{timestamp}.parquet'

    # Collect all data.
    all_events = []
    all_trades = []
    all_quant = []
    all_users = []

    failed_count = 0
    success_count = 0

    # Refetch one by one.
    for idx, (start, end) in enumerate(failed_ranges, 1):
        logger.info(f"[{idx}/{len(failed_ranges)}] Refetching blocks {start}-{end}")

        logs = fetcher.fetch_range_in_batches(start, end)

        if logs is None:
            logger.error("  ✗ RPC request failed")
            failed_count += 1
            continue

        if not logs:
            logger.info("  ✓ No trade data")
            success_count += 1
            continue

        # Decode.
        decoded = decoder.decode_batch(logs)
        formatted = decoder.format_batch(decoded)

        if not formatted:
            logger.info("  ✓ No valid data after decoding")
            success_count += 1
            continue

        # Append to collections.
        all_events.extend(formatted)

        # Generate trades.
        trades_df = extract_trades(formatted, token_mapping)
        if not trades_df.empty:
            all_trades.append(trades_df)

            # Generate quant.
            quant_df = clean_trades_df(trades_df)
            if not quant_df.empty:
                all_quant.append(quant_df)

            # Generate users.
            users_df = clean_users_df(trades_df)
            if not users_df.empty:
                all_users.append(users_df)

        logger.info(f"  ✓ Fetched {len(formatted)} events")
        success_count += 1

    # Save data.
    logger.info(f"\nRefetch complete: success {success_count}, failed {failed_count}")

    if all_events:
        logger.info(f"Saving {len(all_events)} orderfilled events...")
        events_df = pd.DataFrame(all_events)
        events_df.to_parquet(orderfilled_file, index=False, compression='snappy')
        logger.info(f"  ✓ Saved to: {orderfilled_file}")

    if all_trades:
        logger.info("Saving trades data...")
        trades_combined = pd.concat(all_trades, ignore_index=True)
        trades_combined.to_parquet(trades_file, index=False, compression='snappy')
        logger.info(f"  ✓ Saved {len(trades_combined)} trades to: {trades_file}")

    if all_quant:
        logger.info("Saving quant data...")
        quant_combined = pd.concat(all_quant, ignore_index=True)
        quant_combined.to_parquet(quant_file, index=False, compression='snappy')
        logger.info(f"  ✓ Saved {len(quant_combined)} quant rows to: {quant_file}")

    if all_users:
        logger.info("Saving users data...")
        users_combined = pd.concat(all_users, ignore_index=True)
        users_combined.to_parquet(users_file, index=False, compression='snappy')
        logger.info(f"  ✓ Saved {len(users_combined)} users rows to: {users_file}")

    logger.info("\nAll done!")


if __name__ == '__main__':
    main()
