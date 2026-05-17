#!/usr/bin/env python3
"""
Script for continuously fetching the latest block data.

Features:
1. Continuously monitor the blockchain and fetch the latest block data
2. Automatically switch modes: batch historical fetching -> real-time tracking of new blocks (one every 2 seconds)
3. Stream appends into 4 parquet files: orderfilled, trades, users, quant
4. Exit gracefully to preserve file integrity

Usage:
    # Run in the background
    nohup python scripts/continuous_fetch.py > logs/continuous_fetch.log 2>&1 &

    # Stop gracefully
    kill -SIGTERM <PID>

    # Custom output directory
    python scripts/continuous_fetch.py --output-dir data/realtime
"""
import os
import sys
from pathlib import Path
import time
import signal
import argparse
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Add the project root to the path.
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

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


class ContinuousWriter:
    """Manager for continuously appending to parquet files."""

    def __init__(self, output_dir, session_timestamp, preview_size=1000):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session_timestamp = session_timestamp
        self.preview_size = preview_size

        # Four output files with timestamps.
        self.files = {
            'orderfilled': self.output_dir / f'orderfilled_{session_timestamp}.parquet',
            'trades': self.output_dir / f'trades_{session_timestamp}.parquet',
            'quant': self.output_dir / f'quant_{session_timestamp}.parquet',
            'users': self.output_dir / f'users_{session_timestamp}.parquet',
        }

        # CSV preview files with fixed names, updated in real time.
        preview_dir = self.output_dir.parent / 'latest_result'
        preview_dir.mkdir(parents=True, exist_ok=True)
        self.csv_files = {
            'orderfilled': preview_dir / 'orderfilled.csv',
            'trades': preview_dir / 'trades.csv',
            'quant': preview_dir / 'quant.csv',
            'users': preview_dir / 'users.csv',
        }

        # ParquetWriter instances.
        self.writers = {
            'orderfilled': None,
            'trades': None,
            'quant': None,
            'users': None,
        }

        # Row counters.
        self.row_counts = {
            'orderfilled': 0,
            'trades': 0,
            'quant': 0,
            'users': 0,
        }

        # Cache recent data for CSV previews.
        self.recent_data = {
            'orderfilled': [],
            'trades': [],
            'quant': [],
            'users': [],
        }

        # Output file information.
        logger.info("Files for this session:")
        for name, file_path in self.files.items():
            logger.info(f"  {name}: {file_path.name}")
        logger.info(f"CSV preview: data/latest_result/ (latest {preview_size} rows)")

    def write_batch(self, data_type, data):
        """Append a batch of data."""
        if data is None or len(data) == 0:
            return

        # Convert to DataFrame.
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, pd.DataFrame):
            df = data
        else:
            return

        if len(df) == 0:
            return

        try:
            # Convert to Arrow Table.
            table = pa.Table.from_pandas(df)

            # Initialize the writer if needed.
            if self.writers[data_type] is None:
                file_path = self.files[data_type]

                # Create a new file.
                self.writers[data_type] = pq.ParquetWriter(
                    file_path,
                    table.schema,
                    compression='snappy',
                )
                logger.info(f"✓ Created new file {data_type}: {file_path.name}")

            # Write data.
            self.writers[data_type].write_table(table)
            self.row_counts[data_type] += len(df)

            # Update cache and keep the most recent preview_size rows.
            if isinstance(data, list):
                self.recent_data[data_type].extend(data)
            else:
                self.recent_data[data_type].extend(df.to_dict('records'))

            # Keep only the latest N rows.
            if len(self.recent_data[data_type]) > self.preview_size:
                self.recent_data[data_type] = self.recent_data[data_type][-self.preview_size:]

            # Update CSV preview.
            self._update_csv_preview(data_type)

        except Exception as e:
            logger.error(f"Failed to write {data_type}: {e}")
            raise

    def _update_csv_preview(self, data_type):
        """Update the CSV preview file."""
        try:
            if len(self.recent_data[data_type]) > 0:
                df = pd.DataFrame(self.recent_data[data_type])
                csv_file = self.csv_files[data_type]
                df.to_csv(csv_file, index=False)
        except Exception as e:
            logger.warning(f"Failed to update CSV preview ({data_type}): {e}")

    def close_all(self):
        """Close all writers."""
        logger.info("Closing all files...")
        for name, writer in self.writers.items():
            if writer is not None:
                try:
                    writer.close()
                    logger.info(f"  ✓ {name}: {self.row_counts[name]:,} rows")
                except Exception as e:
                    logger.error(f"  Failed to close {name}: {e}")
        logger.info("All files closed")


class ContinuousFetcher:
    """Continuously fetch block data."""

    def __init__(self, output_dir, batch_size=100):
        self.output_dir = Path(output_dir)
        self.batch_size = batch_size

        # State file.
        self.state_file = self.output_dir / 'continuous_state.json'

        # Generate the timestamp for this session.
        self.session_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Log of ranges that could not be fetched even after retries.
        # refetch_failed_blocks.py can later backfill these to close gaps.
        self.failed_blocks_file = self.output_dir / f'failed_blocks_{self.session_timestamp}.txt'

        # Writer.
        self.writer = ContinuousWriter(output_dir, self.session_timestamp)

        # Initialize fetcher and decoder.
        self.fetcher = LogFetcher()
        self.decoder = EventDecoder()

        # Load token mappings.
        self.token_mapping = load_token_mapping(MARKETS_FILE)
        if MISSING_MARKETS_FILE.exists():
            self.token_mapping.update(load_token_mapping(MISSING_MARKETS_FILE))
        logger.info(f"Loaded {len(self.token_mapping)} token mappings")

        # Load state.
        self.last_processed_block = self.load_state()

        # Signal handling.
        self.should_stop = False
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle stop signals."""
        logger.info(f"\nReceived stop signal ({signum}), preparing for a safe shutdown...")
        self.should_stop = True

    def load_state(self):
        """Load the last processed block number."""
        if self.state_file.exists():
            import json
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    return state.get('last_block', None)
            except:
                return None
        return None

    def save_state(self, block_number):
        """Save the current processed block number."""
        import json
        try:
            with open(self.state_file, 'w') as f:
                json.dump({
                    'last_block': block_number,
                    'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def get_latest_block(self):
        """Get the latest on-chain block number."""
        try:
            latest = self.fetcher.client.get_latest_block()
            return latest
        except Exception as e:
            logger.error(f"Failed to get the latest block: {e}")
            return None

    def _process_with_retries(self, start_block, end_block, max_attempts=3, backoff=5.0):
        """Retry a range on RPC failure before giving up.

        Returns True on success, False if the range still fails after all
        attempts — in which case the range is logged to failed_blocks so a
        later backfill pass can recover it.
        """
        for attempt in range(1, max_attempts + 1):
            if self.fetch_and_process_range(start_block, end_block):
                return True
            if attempt < max_attempts:
                sleep_for = backoff * attempt
                logger.warning(
                    f"  Retry {attempt}/{max_attempts - 1} for blocks "
                    f"{start_block:,}-{end_block:,} in {sleep_for:.0f}s"
                )
                time.sleep(sleep_for)
        self._record_failed_range(start_block, end_block)
        return False

    def _record_failed_range(self, start_block, end_block):
        """Persist a block range that could not be fetched after retries."""
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            with open(self.failed_blocks_file, 'a') as f:
                f.write(f"{start_block}-{end_block}\n")
            logger.error(
                f"  ✗ Recorded unresolvable range to {self.failed_blocks_file.name}; "
                f"run refetch_failed_blocks.py later to backfill"
            )
        except Exception as e:
            logger.error(f"  Failed to record failed range {start_block}-{end_block}: {e}")

    def fetch_and_process_range(self, start_block, end_block):
        """Fetch and process a block range.

        Returns:
            True  — fetch succeeded (possibly with zero matching events)
            False — RPC failure; caller should NOT advance state past this range
        """
        try:
            # Fetch logs. fetch_range_in_batches already retries each batch
            # and bisects on persistent failure, so None here means we truly
            # could not retrieve the data — never treat that as "no events."
            logs = self.fetcher.fetch_range_in_batches(start_block, end_block)
            if logs is None:
                logger.error(f"  ✗ RPC failed for blocks {start_block:,}-{end_block:,}")
                return False

            if len(logs) == 0:
                logger.info(f"Block range {start_block:,}-{end_block:,} has no OrderFilled events")
                return True

            logger.info(f"  Fetched {len(logs)} logs")

            # Decode and format events.
            decoded = [self.decoder.decode(log) for log in logs]
            events = [self.decoder.format_event(e) for e in decoded]
            if len(events) == 0:
                logger.info("  No data after decoding")
                return True

            logger.info(f"  Decoded {len(events)} events")

            # Extract trades.
            trades = extract_trades(events)
            logger.info(f"  Extracted {len(trades)} trades")

            # Write orderfilled and trades.
            self.writer.write_batch('orderfilled', events)
            self.writer.write_batch('trades', trades)

            # Clean data only when trade data exists.
            if len(trades) > 0:
                trades_df = pd.DataFrame(trades)
                quant_df = clean_trades_df(trades_df)
                users_df = clean_users_df(trades_df)
                self.writer.write_batch('quant', quant_df)
                self.writer.write_batch('users', users_df)

            logger.info("  ✓ Wrote all data")
            return True

        except Exception as e:
            logger.error(f"Failed to process blocks {start_block}-{end_block}: {e}")
            return False

    def run(self):
        """Main loop: continuously fetch new blocks."""
        logger.info("\n" + "="*60)
        logger.info("=== Continuous fetch mode started ===")
        logger.info("="*60)
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Batch size: {self.batch_size} blocks")
        logger.info("Press Ctrl+C or send SIGTERM to exit gracefully")
        logger.info("="*60 + "\n")

        # Determine the starting block.
        if self.last_processed_block is None:
            latest_block = self.get_latest_block()
            if latest_block is None:
                logger.error("Unable to get the latest block, exiting")
                return
            # Start 100 blocks behind the latest block.
            self.last_processed_block = latest_block - self.batch_size
            logger.info(f"First run, starting from block {self.last_processed_block:,}\n")
        else:
            logger.info(f"Continuing from block {self.last_processed_block:,}\n")

        consecutive_errors = 0
        max_errors = 10
        last_log_time = time.time()

        try:
            while not self.should_stop:
                try:
                    # Get the latest block.
                    latest_block = self.get_latest_block()
                    if latest_block is None:
                        consecutive_errors += 1
                        if consecutive_errors >= max_errors:
                            logger.error(f"Failed to get the latest block {max_errors} times in a row")
                            break
                        time.sleep(5)
                        continue

                    consecutive_errors = 0
                    next_block = self.last_processed_block + 1

                    # Check whether there are new blocks.
                    if next_block > latest_block:
                        # Already up to date, wait 2 seconds.
                        if time.time() - last_log_time > 30:
                            logger.info(f"[Realtime mode] Current: {self.last_processed_block:,}, latest: {latest_block:,}, waiting for new blocks...")
                            last_log_time = time.time()
                        time.sleep(2)
                        continue

                    # Compute the range to process.
                    blocks_behind = latest_block - self.last_processed_block

                    if blocks_behind >= self.batch_size:
                        # Batch mode: process 100 blocks at a time.
                        end_block = next_block + self.batch_size - 1
                        logger.info(f"[Batch mode] Processing {next_block:,} - {end_block:,} (behind by {blocks_behind:,} blocks)")
                        success = self._process_with_retries(next_block, end_block)

                        if success:
                            self.last_processed_block = end_block
                            self.save_state(end_block)
                            logger.info(f"✓ Updated state: {end_block:,}\n")
                        else:
                            # _process_with_retries already recorded the range
                            # to failed_blocks_*.txt; advance so the loop keeps
                            # up with the chain instead of stalling forever.
                            self.last_processed_block = end_block
                            self.save_state(end_block)
                            logger.warning(
                                f"⚠ Advancing past unresolvable range {next_block:,}-{end_block:,}; "
                                f"backfill from {self.failed_blocks_file.name}\n"
                            )

                        # Continue to the next batch without waiting.
                        time.sleep(0.5)
                    else:
                        # Realtime mode: process 1 block at a time.
                        end_block = next_block
                        logger.info(f"[Realtime mode] Processing block {next_block:,} (latest: {latest_block:,})")
                        success = self._process_with_retries(next_block, end_block)

                        if success:
                            self.last_processed_block = end_block
                            self.save_state(end_block)
                            logger.info(f"✓ Updated state: {end_block:,}\n")
                        else:
                            self.last_processed_block = end_block
                            self.save_state(end_block)
                            logger.warning(
                                f"⚠ Advancing past unresolvable block {end_block:,}; "
                                f"backfill from {self.failed_blocks_file.name}\n"
                            )

                        # Realtime mode: wait 2 seconds.
                        last_log_time = time.time()
                        time.sleep(2)

                except Exception as e:
                    logger.error(f"Loop error: {e}")
                    consecutive_errors += 1
                    if consecutive_errors >= max_errors:
                        logger.error(f"Encountered {max_errors} consecutive errors, exiting")
                        break
                    time.sleep(5)

        finally:
            # Graceful shutdown.
            logger.info("\n" + "="*60)
            logger.info("Shutting down safely...")
            self.writer.close_all()
            logger.info("="*60)
            logger.info("=== Continuous fetch mode exited safely ===")
            logger.info("="*60 + "\n")


def main():
    parser = argparse.ArgumentParser(description='Continuously fetch the latest block data')
    parser.add_argument('--output-dir', type=str, default='data/continuous',
                       help='Output directory, default: data/continuous')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Number of blocks fetched per batch in batch mode, default: 100')

    args = parser.parse_args()

    fetcher = ContinuousFetcher(
        output_dir=args.output_dir,
        batch_size=args.batch_size
    )

    fetcher.run()


if __name__ == '__main__':
    main()
