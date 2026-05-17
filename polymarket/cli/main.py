#!/usr/bin/env python3
"""
poly_onchain command-line tool

Usage:
    python -m poly_onchain.cli fetch-onchain --blocks 1000
    python -m poly_onchain.cli fetch-markets
    python -m poly_onchain.cli process
    python -m poly_onchain.cli update
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..config import (
    DATA_DIR, LOG_DIR, STATE_FILE,
    DATASET_DIR, LATEST_RESULT_DIR, DATA_CLEAN_DIR,
    DECODED_EVENTS_FILE, MARKETS_FILE, MISSING_MARKETS_FILE,
    TRADES_OUTPUT_FILE, TRADES_PREVIEW_FILE,
    MARKETS_PREVIEW_FILE, ORDERFILLED_PREVIEW_FILE,
    USERS_CLEAN_FILE, QUANT_CLEAN_FILE,
    USERS_PREVIEW_FILE, QUANT_PREVIEW_FILE
)
from ..fetchers import LogFetcher, GammaApiClient
from ..processors import (
    EventDecoder, extract_trades,
    load_token_mapping, find_missing_tokens, save_preview_csv,
    clean_users, clean_trades, clean_users_df, clean_trades_df
)

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False):
    """Set up logging to both the console and file"""
    log_level = logging.DEBUG if verbose else logging.INFO
    log_format = '%(asctime)s [%(levelname)s] %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    # Create the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Clear existing handlers to avoid duplication
    root_logger.handlers.clear()

    # File handler for primary log output
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / 'poly_onchain.log'
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(logging.Formatter(log_format, date_format))
    root_logger.addHandler(file_handler)

    # Console handler, only when not running under nohup
    # Check whether an interactive terminal is available
    if sys.stdout.isatty():
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(logging.Formatter(log_format, date_format))
        root_logger.addHandler(console_handler)


def get_last_block() -> int:
    """Get the last processed block

    Priority:
    1. `last_block` in `STATE_FILE` (most accurate, records the actual processed block)
    2. Maximum `block_number` in `orderfilled.parquet` (fallback)
    3. Return 0 (first run)
    """
    # First try reading from the state file
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                state = json.load(f)

                # New format: fetch_onchain.last_block
                if 'fetch_onchain' in state:
                    last_block = state['fetch_onchain'].get('last_block', 0)
                    if last_block > 0:
                        return last_block

                # Backward compatibility: direct last_block
                last_block = state.get('last_block', 0)
                if last_block > 0:
                    return last_block
        except (json.JSONDecodeError, IOError):
            pass

    # Fallback: read the maximum block number from the parquet file
    if DECODED_EVENTS_FILE.exists():
        try:
            # Read the block_number column from the whole file to find the maximum
            df = pq.read_table(DECODED_EVENTS_FILE, columns=['block_number']).to_pandas()
            if len(df) > 0:
                return int(df['block_number'].max())
        except Exception:
            pass

    return 0


def save_last_block(block: int):
    """Save the last processed block to state.json"""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Read existing state
    state = {}
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                state = json.load(f)
        except:
            pass

    # Update fetch_onchain state
    state['fetch_onchain'] = {
        'last_block': block,
        'updated_at': datetime.now().isoformat()
    }

    # Save
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def cmd_fetch_onchain(args):
    """Fetch on-chain data (incremental mode)

    Features:
    - Uses PyArrow streaming writes without reading historical data
    - Supports safe exit with Ctrl+C
    - Supports resume with --continue
    """
    import signal

    # Input validation
    MAX_BLOCKS = 1_000_000  # maximum allowed block count
    MIN_BLOCK = 1  # minimum block number

    if args.blocks is not None:
        if args.blocks <= 0:
            logger.error(f"--blocks must be a positive integer, current value: {args.blocks}")
            return
        if args.blocks > MAX_BLOCKS:
            logger.error(f"--blocks exceeds the limit, maximum: {MAX_BLOCKS}, current value: {args.blocks}")
            return

    if args.range is not None:
        start_r, end_r = args.range
        if start_r < MIN_BLOCK or end_r < MIN_BLOCK:
            logger.error(f"Block number must be >= {MIN_BLOCK}")
            return
        if start_r > end_r:
            logger.error(f"Start block ({start_r}) cannot be greater than end block ({end_r})")
            return
        if end_r - start_r + 1 > MAX_BLOCKS:
            logger.error(f"Block range exceeds the limit, maximum: {MAX_BLOCKS}, current range: {end_r - start_r + 1}")
            return

    fetcher = LogFetcher(use_alchemy=args.alchemy)
    decoder = EventDecoder()

    # Determine the block range
    if args.continue_from:
        start = get_last_block() + 1
        end = fetcher.get_latest_block()
    elif args.blocks:
        end = fetcher.get_latest_block()
        start = end - args.blocks + 1
    elif args.range:
        start, end = args.range
    else:
        logger.error("Please specify --blocks, --range, or --continue")
        return

    if start > end:
        logger.info("No new blocks")
        return

    logger.info(f"Fetching blocks {start} - {end} (total {end - start + 1} blocks)")

    # Create directories
    DATASET_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_RESULT_DIR.mkdir(parents=True, exist_ok=True)
    DATA_CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    # Load token mappings used for generating trades
    token_mapping = load_token_mapping(MARKETS_FILE)
    if MISSING_MARKETS_FILE.exists():
        token_mapping.update(load_token_mapping(MISSING_MARKETS_FILE))
    logger.info(f"Loaded {len(token_mapping)} token mappings")

    # Safe-exit flag
    stop_requested = False
    last_saved_block = start - 1

    def signal_handler(signum, frame):
        nonlocal stop_requested
        logger.warning(f"Received exit signal ({signum}), will exit safely after the current batch finishes...")
        stop_requested = True

    # Register signal handlers
    original_sigint = signal.signal(signal.SIGINT, signal_handler)
    original_sigterm = signal.signal(signal.SIGTERM, signal_handler)

    # PyArrow streaming writers (append mode)
    events_writer = None
    trades_writer = None
    quant_writer = None
    users_writer = None

    # Use timestamped session filenames to avoid overwriting existing data
    session_ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    events_temp = DATASET_DIR / f'orderfilled_session_{session_ts}.parquet'
    trades_temp = DATASET_DIR / f'trades_session_{session_ts}.parquet'
    quant_temp = DATA_CLEAN_DIR / f'quant_session_{session_ts}.parquet'
    users_temp = DATA_CLEAN_DIR / f'users_session_{session_ts}.parquet'

    logger.info(f"Session file prefix: session_{session_ts}")

    def close_writers():
        """Close all writers and ensure data is flushed to disk"""
        nonlocal events_writer, trades_writer, quant_writer, users_writer
        for w in [events_writer, trades_writer, quant_writer, users_writer]:
            if w:
                try:
                    w.close()
                except:
                    pass
        events_writer = trades_writer = quant_writer = users_writer = None


    def merge_temp_files():
        """Merge the current session files into the main files (current session only)"""
        import shutil
        for temp_file, main_file in [
            (events_temp, DECODED_EVENTS_FILE),
            (trades_temp, TRADES_OUTPUT_FILE),
            (quant_temp, QUANT_CLEAN_FILE),
            (users_temp, USERS_CLEAN_FILE)
        ]:
            if not temp_file.exists():
                continue
            if main_file.exists():
                # Merge
                temp_table = pq.read_table(temp_file)
                main_table = pq.read_table(main_file)
                combined = pa.concat_tables([main_table, temp_table])
                pq.write_table(combined, main_file, compression='snappy')
                temp_file.unlink()
                del temp_table, main_table, combined
            else:
                # Move directly
                shutil.move(str(temp_file), str(main_file))

    # Fetch in batches and save every 100 blocks
    batch_size = 100
    checkpoint_interval = 10  # Save a checkpoint every 10 batches (about 1000 blocks, 33 minutes of data)
    current = start
    total_events = 0
    total_trades = 0
    total_quant = 0
    total_users = 0
    batches_since_checkpoint = 0

    # Record failed block ranges
    failed_blocks_file = DATA_DIR / f'failed_blocks_{session_ts}.txt'
    failed_count = 0

    def record_failed_block(block_start, block_end, reason=""):
        """Record a failed block range"""
        nonlocal failed_count
        with open(failed_blocks_file, 'a') as f:
            f.write(f"{block_start}-{block_end}\n")
        failed_count += 1
        logger.warning(f"Recorded failed block range: {block_start}-{block_end} {reason}")

    try:
        while current <= end:
            # Check for exit signal
            if stop_requested:
                logger.info("Received exit signal, saving progress and exiting...")
                break

            batch_end = min(current + batch_size - 1, end)

            logs = fetcher.fetch_range_in_batches(current, batch_end)
            if logs is None:
                # RPC request failed, record it and continue with the next batch
                record_failed_block(current, batch_end, "(RPC failed)")
                # Do not update last_saved_block here because this batch was not fetched successfully
                current = batch_end + 1
                batches_since_checkpoint += 1
                continue

            # logs is a list and may be empty, meaning no trade events in this block range
            if logs:
                decoded = decoder.decode_batch(logs)
                formatted = decoder.format_batch(decoded)

                if formatted:
                    # New data containing only the current batch
                    new_df = pd.DataFrame(formatted)
                    batch_events = len(new_df)

                    # 1. Write orderfilled by streaming append into the temp file
                    events_table = pa.Table.from_pandas(new_df, preserve_index=False)
                    if events_writer is None:
                        events_writer = pq.ParquetWriter(str(events_temp), events_table.schema, compression='snappy')
                    events_writer.write_table(events_table)
                    new_df.tail(1000).to_csv(ORDERFILLED_PREVIEW_FILE, index=False)

                    # 2. Generate trades
                    events = new_df.to_dict('records')
                    trades_df = extract_trades(events, token_mapping)
                    batch_trades = 0
                    batch_quant = 0
                    batch_users = 0

                    if not trades_df.empty:
                        batch_trades = len(trades_df)

                        # Write trades
                        trades_table = pa.Table.from_pandas(trades_df, preserve_index=False)
                        if trades_writer is None:
                            trades_writer = pq.ParquetWriter(str(trades_temp), trades_table.schema, compression='snappy')
                        trades_writer.write_table(trades_table)
                        trades_df.tail(1000).to_csv(TRADES_PREVIEW_FILE, index=False)

                        # 3. Generate quant
                        quant_df = clean_trades_df(trades_df)
                        if not quant_df.empty:
                            batch_quant = len(quant_df)
                            quant_table = pa.Table.from_pandas(quant_df, preserve_index=False)
                            if quant_writer is None:
                                quant_writer = pq.ParquetWriter(str(quant_temp), quant_table.schema, compression='snappy')
                            quant_writer.write_table(quant_table)
                            quant_df.tail(1000).to_csv(QUANT_PREVIEW_FILE, index=False)

                        # 4. Generate users
                        users_df = clean_users_df(trades_df)
                        if not users_df.empty:
                            batch_users = len(users_df)
                            users_table = pa.Table.from_pandas(users_df, preserve_index=False)
                            if users_writer is None:
                                users_writer = pq.ParquetWriter(str(users_temp), users_table.schema, compression='snappy')
                            users_writer.write_table(users_table)
                            users_df.tail(1000).to_csv(USERS_PREVIEW_FILE, index=False)

                    total_events += batch_events
                    total_trades += batch_trades
                    total_quant += batch_quant
                    total_users += batch_users

                    logger.info(f"Blocks {current}-{batch_end}: "
                               f"events+{batch_events}, trades+{batch_trades}, "
                               f"quant+{batch_quant}, users+{batch_users}")

            last_saved_block = batch_end
            current = batch_end + 1
            batches_since_checkpoint += 1

            # Save checkpoints periodically to avoid losing progress on crashes
            if batches_since_checkpoint >= checkpoint_interval:
                save_last_block(last_saved_block)
                logger.info(f"  ✓ Checkpoint saved (block {last_saved_block})")
                batches_since_checkpoint = 0

        # Close writers
        close_writers()

        # Decide whether to merge based on arguments
        if total_events > 0 and getattr(args, 'merge', False):
            logger.info("Merging data into main files...")
            merge_temp_files()
        elif total_events > 0:
            logger.info(f"Data saved to temporary files. Use --merge to merge into the main files")

        # Save the last processed block
        save_last_block(last_saved_block)

        logger.info(f"On-chain data fetch complete, added: events {total_events}, trades {total_trades}, "
                   f"quant {total_quant}, users {total_users}")

        # Report failure statistics
        if failed_count > 0:
            logger.warning(f"Warning: {failed_count} block batches failed to fetch and were recorded in: {failed_blocks_file}")
            logger.info(f"You can refetch these blocks later with --range")

    except Exception as e:
        logger.error(f"Error fetching on-chain data: {e}")
        close_writers()
        # Save progress even if an error occurs
        if last_saved_block >= start:
            save_last_block(last_saved_block)
        raise

    finally:
        # Restore original signal handlers
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)
        close_writers()


def cmd_fetch_markets(args):
    """Incrementally fetch new markets (high-frequency, e.g. hourly)

    Features:
    - Incrementally fetch new markets without refetching existing ones
    - Supports safe exit with Ctrl+C
    - Supports resume with --continue
    - Market data is small, so it is processed in memory and saved in one shot
    """
    import signal

    client = GammaApiClient()

    if not client.test_connection():
        logger.error("API connection failed")
        return

    # Create directories
    DATASET_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_RESULT_DIR.mkdir(parents=True, exist_ok=True)

    # Load the existing market ID set (IDs only, not full data)
    existing_ids = set()
    if MARKETS_FILE.exists():
        existing_df = pq.read_table(MARKETS_FILE, columns=['id']).to_pandas()
        existing_ids = set(existing_df['id'].astype(str).tolist())
        logger.info(f"Already have {len(existing_ids)} markets")

    # Safe-exit flag
    stop_requested = False

    def signal_handler(signum, frame):
        nonlocal stop_requested
        logger.warning(f"Received exit signal ({signum}), will exit safely after the current batch finishes...")
        stop_requested = True

    original_sigint = signal.signal(signal.SIGINT, signal_handler)
    original_sigterm = signal.signal(signal.SIGTERM, signal_handler)

    # Read checkpoint
    continue_from = getattr(args, 'continue_from', False)
    offset = 0
    if continue_from and STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                state = json.load(f)
                markets_state = state.get('fetch_markets', {})
                offset = markets_state.get('last_offset', 0)
                if offset > 0:
                    logger.info(f"Continuing from offset={offset}")
        except Exception as e:
            logger.warning(f"Failed to read checkpoint: {e}")
            offset = 0

    logger.info("Incrementally fetching new markets...")
    new_markets = []  # Store only new markets.
    new_count = 0
    consecutive_existing = 0
    batch_size = 500

    try:
        while True:
            if stop_requested:
                logger.info("Received exit signal, saving progress and exiting...")
                break

            logger.info(f"Fetching offset={offset}...")
            markets = client.get_markets(limit=batch_size, offset=offset)

            if not markets:
                break

            batch_new = 0

            for market in markets:
                market_id = str(market['id'])
                if market_id not in existing_ids:
                    # New market.
                    new_markets.append(market)
                    existing_ids.add(market_id)
                    batch_new += 1
                    new_count += 1
                    consecutive_existing = 0
                else:
                    consecutive_existing += 1

            if batch_new > 0:
                logger.info(f"  New markets in this batch: {batch_new} markets")

            # Stop if three consecutive batches contain only existing markets
            if consecutive_existing >= batch_size * 3:
                logger.info("Only existing markets encountered consecutively, incremental sync complete")
                break

            if len(markets) < batch_size:
                break

            offset += len(markets)
            time.sleep(0.3)

        # Save new markets by appending to the main file.
        if new_markets:
            logger.info(f"Saving {len(new_markets)} new markets...")
            new_df = pd.DataFrame(new_markets)

            if MARKETS_FILE.exists():
                # Append to the existing file
                existing_table = pq.read_table(MARKETS_FILE)
                new_table = pa.Table.from_pandas(new_df, preserve_index=False)
                combined = pa.concat_tables([existing_table, new_table])
                pq.write_table(combined, MARKETS_FILE, compression='snappy')
                del existing_table, combined
            else:
                new_df.to_parquet(MARKETS_FILE, index=False)

            # Update preview by reading the latest 1000 rows from the file
            preview_df = pq.read_table(MARKETS_FILE).to_pandas().tail(1000)
            preview_df.to_csv(MARKETS_PREVIEW_FILE, index=False)

        # Save checkpoint.
        state = {}
        if STATE_FILE.exists():
            try:
                with open(STATE_FILE) as f:
                    state = json.load(f)
            except:
                pass

        state['fetch_markets'] = {
            'last_offset': offset,
            'total_markets': len(existing_ids),
            'new_count': new_count,
            'updated_at': datetime.now().isoformat()
        }
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)

        logger.info(f"Done! Total markets: {len(existing_ids)} markets (new: {new_count})")

    finally:
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)


def cmd_update_markets(args):
    """Update the status of markets that are not closed (low-frequency, e.g. weekly)

    Features:
    - Update only markets that are not closed
    - Supports safe exit with Ctrl+C
    - Supports resume with --continue
    - Market data is small enough that the full file is loaded for updates

    Note: the market file is small (~60MB), so full loading is required to update specific markets
    """
    import signal

    client = GammaApiClient()

    if not client.test_connection():
        logger.error("API connection failed")
        return

    if not MARKETS_FILE.exists():
        logger.error(f"Markets file does not exist: {MARKETS_FILE}")
        return

    # Load existing data (the market file is small enough to load fully)
    df = pd.read_parquet(MARKETS_FILE)
    markets_dict = {str(row['id']): dict(row) for _, row in df.iterrows()}

    # Filter markets that are not closed
    unclosed_ids = []
    for mid, m in markets_dict.items():
        is_closed = m.get('closed', m.get('resolved', False))
        if not is_closed:
            unclosed_ids.append(mid)

    logger.info(f"Total markets: {len(markets_dict)}, not closed: {len(unclosed_ids)}")

    if not unclosed_ids:
        logger.info("No markets need updating")
        return

    # Safe-exit flag
    stop_requested = False

    def signal_handler(signum, frame):
        nonlocal stop_requested
        logger.warning(f"Received exit signal ({signum}), will exit safely after the current market finishes...")
        stop_requested = True

    original_sigint = signal.signal(signal.SIGINT, signal_handler)
    original_sigterm = signal.signal(signal.SIGTERM, signal_handler)

    # Read checkpoint
    continue_from = getattr(args, 'continue_from', False)
    start_idx = 0
    if continue_from and STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                state = json.load(f)
                update_state = state.get('update_markets', {})
                start_idx = update_state.get('last_index', 0)
                if start_idx > 0:
                    logger.info(f"Continuing from market #{start_idx}")
        except Exception as e:
            logger.warning(f"Failed to read checkpoint: {e}")
            start_idx = 0

    updated_count = 0
    closed_count = 0
    last_saved_idx = start_idx - 1

    def save_progress(idx):
        """Save progress and data"""
        df_updated = pd.DataFrame(list(markets_dict.values()))
        pq.write_table(
            pa.Table.from_pandas(df_updated, preserve_index=False),
            MARKETS_FILE,
            compression='snappy'
        )
        df_updated.tail(1000).to_csv(MARKETS_PREVIEW_FILE, index=False)

        state = {}
        if STATE_FILE.exists():
            try:
                with open(STATE_FILE) as f:
                    state = json.load(f)
            except:
                pass
        state['update_markets'] = {
            'last_index': idx + 1,
            'total_unclosed': len(unclosed_ids),
            'updated_count': updated_count,
            'closed_count': closed_count,
            'updated_at': datetime.now().isoformat()
        }
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)

    try:
        for idx, market_id in enumerate(unclosed_ids[start_idx:], start=start_idx):
            if stop_requested:
                logger.info("Received exit signal, saving progress and exiting...")
                break

            logger.info(f"Updating market {idx+1}/{len(unclosed_ids)}: {market_id[:20]}...")

            old_market = markets_dict[market_id]
            token_id = old_market.get('token1', '')

            if not token_id:
                logger.warning(f"Market {market_id} has no token_id, skipping")
                last_saved_idx = idx
                continue

            new_market = client.get_market_by_token(token_id)

            if new_market and new_market['id'] == market_id:
                markets_dict[market_id] = new_market
                updated_count += 1

                if new_market.get('closed', False):
                    closed_count += 1
                    logger.info(f"  ✓ Market is now closed")

            last_saved_idx = idx

            # Save every 50 markets
            if (idx + 1) % 50 == 0:
                save_progress(idx)
                logger.info(f"Progress: {idx+1}/{len(unclosed_ids)} (updated {updated_count}, newly closed {closed_count})")

            time.sleep(0.3)

        # Final save
        if last_saved_idx >= start_idx:
            save_progress(last_saved_idx)

        logger.info(f"Done! Updated {updated_count} markets, {closed_count} are now closed")

    finally:
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)


def cmd_process_historical(args):
    """Process historical data in batches (for large files, to avoid out-of-memory issues)

    Usage:
        python3 run.py process-historical --batch-size 1000000
        python3 run.py process-historical --continue  # Continue from the last checkpoint

    Notes:
        - Read orderfilled.parquet in batches
        - Use PyArrow streaming writes without reading existing data
        - Support resume after interruption
        - Safe exit: Ctrl+C closes writers and saves after the current batch finishes
    """
    import signal
    import gc

    if not DECODED_EVENTS_FILE.exists():
        logger.error(f"Events file does not exist: {DECODED_EVENTS_FILE}")
        return

    batch_size = getattr(args, 'batch_size', 1000000)  # default 1,000,000 rows per batch
    test_batches = getattr(args, 'test_batches', None)  # test mode
    continue_from = getattr(args, 'continue_from', False)  # resume mode
    checkpoint_interval = 10  # save progress every 10 batches

    # Read previous progress from state.json
    start_batch = 0
    total_trades = 0
    total_quant = 0
    total_users = 0
    session_id = 0  # used to distinguish files from different run sessions

    if continue_from and STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                state = json.load(f)
                process_state = state.get('process_historical', {})
                start_batch = process_state.get('last_batch', -1) + 1
                total_trades = process_state.get('total_trades', 0)
                total_quant = process_state.get('total_quant', 0)
                total_users = process_state.get('total_users', 0)
                session_id = process_state.get('session_id', 0) + 1
                if start_batch > 0:
                    logger.info(f"Continuing from batch {start_batch + 1} (session {session_id})")
                    logger.info(f"  Existing data: trades={total_trades:,}, quant={total_quant:,}, users={total_users:,}")
        except Exception as e:
            logger.warning(f"Failed to read progress: {e}, starting from scratch")
            start_batch = 0

    if test_batches:
        logger.info(f"Test mode: processing batches {start_batch + 1} to {start_batch + test_batches}, with {batch_size:,} rows")
    else:
        if start_batch > 0:
            logger.info(f"Continuing historical batch processing from batch {start_batch + 1}, with {batch_size:,} rows")
        else:
            logger.info(f"Starting historical batch processing, with {batch_size:,} rows")

    # 1. Load token mappings
    logger.info("Loading token mappings...")
    token_mapping = load_token_mapping(MARKETS_FILE)
    if MISSING_MARKETS_FILE.exists():
        token_mapping.update(load_token_mapping(MISSING_MARKETS_FILE))
    logger.info(f"Total token mappings: {len(token_mapping)}")

    # 2. Get total row count
    parquet_file = pq.ParquetFile(DECODED_EVENTS_FILE)
    total_rows = parquet_file.metadata.num_rows
    total_batches = (total_rows + batch_size - 1) // batch_size
    logger.info(f"Total events: {total_rows:,} rows across {total_batches} batches")

    if start_batch >= total_batches:
        logger.info("All data has already been processed")
        return

    # Ensure directories exist
    DATASET_DIR.mkdir(parents=True, exist_ok=True)
    DATA_CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_RESULT_DIR.mkdir(parents=True, exist_ok=True)

    # Safe-exit flag
    stop_requested = False

    def signal_handler(signum, frame):
        nonlocal stop_requested
        logger.warning(f"Received exit signal ({signum}), will exit safely after the current batch finishes...")
        stop_requested = True

    # Register signal handlers
    original_sigint = signal.signal(signal.SIGINT, signal_handler)
    original_sigterm = signal.signal(signal.SIGTERM, signal_handler)

    # Determine output file paths
    # If resuming, write to new session files; otherwise write directly to main files
    if start_batch > 0:
        trades_output = DATASET_DIR / f'trades_session_{session_id}.parquet'
        quant_output = DATA_CLEAN_DIR / f'quant_session_{session_id}.parquet'
        users_output = DATA_CLEAN_DIR / f'users_session_{session_id}.parquet'
    else:
        # Starting from scratch: delete old files
        trades_output = TRADES_OUTPUT_FILE
        quant_output = QUANT_CLEAN_FILE
        users_output = USERS_CLEAN_FILE
        for f in [trades_output, quant_output, users_output]:
            if f.exists():
                f.unlink()

    # PyArrow streaming writers
    trades_writer = None
    quant_writer = None
    users_writer = None

    def save_progress(batch_idx, final=False):
        """Save progress to state.json"""
        state = {}
        if STATE_FILE.exists():
            try:
                with open(STATE_FILE) as f:
                    state = json.load(f)
            except:
                pass

        state['process_historical'] = {
            'last_batch': batch_idx,
            'total_batches': total_batches,
            'total_trades': total_trades,
            'total_quant': total_quant,
            'total_users': total_users,
            'session_id': session_id,
            'updated_at': datetime.now().isoformat()
        }

        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)

    def close_writers():
        """Close all writers"""
        nonlocal trades_writer, quant_writer, users_writer
        if trades_writer:
            trades_writer.close()
            trades_writer = None
        if quant_writer:
            quant_writer.close()
            quant_writer = None
        if users_writer:
            users_writer.close()
            users_writer = None

    def merge_session_files():
        """Merge all session files into the main files"""
        import glob as glob_module

        for main_file, pattern, output_dir in [
            (TRADES_OUTPUT_FILE, 'trades_session_*.parquet', DATASET_DIR),
            (QUANT_CLEAN_FILE, 'quant_session_*.parquet', DATA_CLEAN_DIR),
            (USERS_CLEAN_FILE, 'users_session_*.parquet', DATA_CLEAN_DIR)
        ]:
            session_files = sorted(glob_module.glob(str(output_dir / pattern)))
            if not session_files:
                continue

            # Collect all files (main file + session files)
            all_files = []
            if main_file.exists():
                all_files.append(str(main_file))
            all_files.extend(session_files)

            if len(all_files) <= 1:
                # If there is only one file and it is a session file, rename it to the main file
                if session_files and not main_file.exists():
                    import shutil
                    shutil.move(session_files[0], str(main_file))
                continue

            # Merge all files.
            logger.info(f"Merging {len(all_files)} files into {main_file.name}...")
            tables = [pq.read_table(f) for f in all_files]
            combined = pa.concat_tables(tables)
            pq.write_table(combined, main_file, compression='snappy')

            # Delete session files
            for sf in session_files:
                Path(sf).unlink()

            del tables, combined
            gc.collect()

    try:
        last_completed_batch = start_batch - 1

        for batch_idx, batch in enumerate(parquet_file.iter_batches(batch_size=batch_size)):
            # Check for exit signal
            if stop_requested:
                logger.info("Received exit signal, closing writers and saving progress...")
                close_writers()
                save_progress(last_completed_batch)
                break

            # Skip batches that have already been processed
            if batch_idx < start_batch:
                continue

            # Test mode: process only the specified number of batches.
            if test_batches and (batch_idx - start_batch) >= test_batches:
                logger.info(f"Test mode complete, processed {test_batches} batches")
                break

            batch_start_time = datetime.now()
            batch_df = batch.to_pandas()
            batch_rows = len(batch_df)
            progress_pct = (batch_idx + 1) * 100.0 / total_batches
            logger.info(f"Processing batch {batch_idx + 1}/{total_batches}: {batch_rows:,} event rows ({progress_pct:.1f}%)")

            # Generate trades
            events = batch_df.to_dict('records')
            trades_df = extract_trades(events, token_mapping)

            batch_quant = 0
            batch_users = 0

            if not trades_df.empty:
                batch_trades = len(trades_df)
                total_trades += batch_trades

                # Write trades
                trades_table = pa.Table.from_pandas(trades_df, preserve_index=False)
                if trades_writer is None:
                    trades_writer = pq.ParquetWriter(str(trades_output), trades_table.schema, compression='snappy')
                trades_writer.write_table(trades_table)

                # Generate and write quant
                quant_df = clean_trades_df(trades_df)
                if not quant_df.empty:
                    batch_quant = len(quant_df)
                    total_quant += batch_quant
                    quant_table = pa.Table.from_pandas(quant_df, preserve_index=False)
                    if quant_writer is None:
                        quant_writer = pq.ParquetWriter(str(quant_output), quant_table.schema, compression='snappy')
                    quant_writer.write_table(quant_table)

                # Generate and write users
                users_df = clean_users_df(trades_df)
                if not users_df.empty:
                    batch_users = len(users_df)
                    total_users += batch_users
                    users_table = pa.Table.from_pandas(users_df, preserve_index=False)
                    if users_writer is None:
                        users_writer = pq.ParquetWriter(str(users_output), users_table.schema, compression='snappy')
                    users_writer.write_table(users_table)

                batch_elapsed = (datetime.now() - batch_start_time).total_seconds()
                logger.info(f"  → trades+{batch_trades:,}, quant+{batch_quant:,}, users+{batch_users:,} ({batch_elapsed:.1f}s)")

                # Update CSV previews in real time with the latest 1000 rows
                trades_df.tail(1000).to_csv(TRADES_PREVIEW_FILE, index=False)
                if not quant_df.empty:
                    quant_df.tail(1000).to_csv(QUANT_PREVIEW_FILE, index=False)
                if not users_df.empty:
                    users_df.tail(1000).to_csv(USERS_PREVIEW_FILE, index=False)

            # Mark this batch as completed
            last_completed_batch = batch_idx

            # Save progress every N batches without closing writers.
            batches_processed = batch_idx - start_batch + 1
            if batches_processed > 0 and batches_processed % checkpoint_interval == 0:
                save_progress(batch_idx)
                logger.info(f"  ✓ Progress saved (batch {batch_idx + 1})")

            # Explicitly free memory
            del batch_df, trades_df
            if 'quant_df' in locals():
                del quant_df
            if 'users_df' in locals():
                del users_df
            gc.collect()

        # Normal completion
        if not stop_requested:
            close_writers()
            save_progress(last_completed_batch)

            # Merge all session files.
            if session_id > 0:
                logger.info("Merging all session files...")
                merge_session_files()

            logger.info(f"Historical data processing complete!")
            logger.info(f"  Total: trades {total_trades:,}, quant {total_quant:,}, users {total_users:,}")

    except Exception as e:
        logger.error(f"Processing error: {e}")
        close_writers()
        if last_completed_batch >= start_batch:
            save_progress(last_completed_batch)
            logger.info(f"Saved progress through batch {last_completed_batch + 1}")
        raise

    finally:
        # Restore original signal handlers
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)

        # Ensure writers are closed
        close_writers()


def cmd_process(args):
    """Process trade data with market_id mapping and missing token backfill

    Warning: this command reads all data at once and is only suitable for small datasets!
    For large datasets (>1GB), use the process-historical command instead.
    """
    if not DECODED_EVENTS_FILE.exists():
        logger.error(f"Events file does not exist: {DECODED_EVENTS_FILE}")
        return

    # 1. Load token mappings
    logger.info("Loading token mappings...")
    token_mapping = load_token_mapping(MARKETS_FILE)

    # Also load the missing markets file
    if MISSING_MARKETS_FILE.exists():
        missing_mapping = load_token_mapping(MISSING_MARKETS_FILE)
        token_mapping.update(missing_mapping)
        logger.info(f"Merged missing market mappings, total tokens: {len(token_mapping)}")

    # 2. Read events and extract trades
    logger.info("Reading events...")
    df = pd.read_parquet(DECODED_EVENTS_FILE)
    events = df.to_dict('records')

    logger.info("Extracting trades...")
    trades_df = extract_trades(events, token_mapping)

    if trades_df.empty:
        logger.info("No trade data")
        return

    # 3. Find and backfill missing tokens
    missing_tokens = find_missing_tokens(trades_df, token_mapping)
    if missing_tokens and not getattr(args, 'skip_missing', False):
        logger.info(f"Backfilling {len(missing_tokens)} missing tokens...")
        client = GammaApiClient()
        new_markets = client.fetch_missing_tokens(list(missing_tokens))

        if new_markets:
            # Save to the missing markets file.
            new_df = pd.DataFrame(new_markets)
            MISSING_MARKETS_FILE.parent.mkdir(parents=True, exist_ok=True)

            if MISSING_MARKETS_FILE.exists():
                existing = pd.read_parquet(MISSING_MARKETS_FILE)
                new_df = pd.concat([existing, new_df], ignore_index=True)
                new_df = new_df.drop_duplicates(subset=['id'])

            new_df.to_parquet(MISSING_MARKETS_FILE, index=False)
            logger.info(f"Saved {len(new_markets)} missing markets")

            # Update mappings and reprocess
            for m in new_markets:
                if m.get('token1'):
                    token_mapping[m['token1']] = {'market_id': m['id'], 'answer': m.get('answer1', 'YES')}
                if m.get('token2'):
                    token_mapping[m['token2']] = {'market_id': m['id'], 'answer': m.get('answer2', 'NO')}

            # Re-extract trades with the complete mapping
            logger.info("Re-extracting trades...")
            trades_df = extract_trades(events, token_mapping)

    # 4. Save results
    TRADES_OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    trades_df.to_parquet(TRADES_OUTPUT_FILE, index=False)
    logger.info(f"Saved {len(trades_df)} trades to {TRADES_OUTPUT_FILE}")

    # 5. Save a CSV preview with the latest 1000 rows.
    save_preview_csv(trades_df, TRADES_PREVIEW_FILE, n_rows=1000)

    # 6. Statistics
    matched = (trades_df['market_id'] != '').sum()
    logger.info(f"market_id match rate: {matched}/{len(trades_df)} ({matched/len(trades_df)*100:.1f}%)")


def cmd_clean_users(args):
    """Clean user data"""
    if not TRADES_OUTPUT_FILE.exists():
        logger.error(f"Trades file does not exist: {TRADES_OUTPUT_FILE}")
        logger.info("Please run fetch-onchain and process first to generate trade data")
        return

    DATA_CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    try:
        stats = clean_users(
            input_path=TRADES_OUTPUT_FILE,
            output_path=USERS_CLEAN_FILE,
            batch_size=args.batch_size,
            test_rows=args.test
        )
        logger.info(f"User data saved to: {USERS_CLEAN_FILE}")
    except Exception as e:
        logger.error(f"Failed to clean user data: {e}")
        raise


def cmd_clean_trades(args):
    """Clean trade data (for quant use)"""
    if not TRADES_OUTPUT_FILE.exists():
        logger.error(f"Trades file does not exist: {TRADES_OUTPUT_FILE}")
        logger.info("Please run fetch-onchain and process first to generate trade data")
        return

    DATA_CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    try:
        stats = clean_trades(
            input_path=TRADES_OUTPUT_FILE,
            output_path=QUANT_CLEAN_FILE,
            batch_size=args.batch_size,
            test_rows=args.test
        )
        logger.info(f"Quant trade data saved to: {QUANT_CLEAN_FILE}")
    except Exception as e:
        logger.error(f"Failed to clean trade data: {e}")
        raise


def cmd_clean(args):
    """Run all data cleaning"""
    logger.info("=== Cleaning user data ===")
    cmd_clean_users(args)

    logger.info("\n=== Cleaning quant trade data ===")
    cmd_clean_trades(args)

    logger.info("\nData cleaning complete!")


def cmd_update(args):
    """Full update"""
    logger.info("=== Updating market data ===")
    cmd_fetch_markets(args)

    logger.info("\n=== Updating on-chain data ===")
    args.continue_from = True
    args.blocks = None
    args.range = None
    cmd_fetch_onchain(args)

    logger.info("\n=== Processing trades ===")
    cmd_process(args)

    # If --clean is specified, also run data cleaning
    if getattr(args, 'with_clean', False):
        logger.info("\n=== Cleaning data ===")
        args.batch_size = 5_000_000
        args.test = None
        cmd_clean(args)

    logger.info("\nFull update complete!")


def cmd_merge_sessions(args):
    """Merge all session files into the main files"""
    import glob as glob_module
    import gc

    logger.info("=== Merging session files ===")

    for main_file, pattern, output_dir, file_type in [
        (DECODED_EVENTS_FILE, 'orderfilled_session_*.parquet', DATASET_DIR, 'orderfilled_session'),
        (DECODED_EVENTS_FILE, 'orderfilled_refetched_*.parquet', DATASET_DIR, 'orderfilled_refetched'),
        (DECODED_EVENTS_FILE, 'orderfilled_append.parquet', DATASET_DIR, 'orderfilled_append'),
        (TRADES_OUTPUT_FILE, 'trades_session_*.parquet', DATASET_DIR, 'trades_session'),
        (TRADES_OUTPUT_FILE, 'trades_refetched_*.parquet', DATASET_DIR, 'trades_refetched'),
        (TRADES_OUTPUT_FILE, 'trades_append.parquet', DATASET_DIR, 'trades_append'),
        (QUANT_CLEAN_FILE, 'quant_session_*.parquet', DATA_CLEAN_DIR, 'quant_session'),
        (QUANT_CLEAN_FILE, 'quant_refetched_*.parquet', DATA_CLEAN_DIR, 'quant_refetched'),
        (QUANT_CLEAN_FILE, 'quant_append.parquet', DATA_CLEAN_DIR, 'quant_append'),
        (USERS_CLEAN_FILE, 'users_session_*.parquet', DATA_CLEAN_DIR, 'users_session'),
        (USERS_CLEAN_FILE, 'users_refetched_*.parquet', DATA_CLEAN_DIR, 'users_refetched'),
        (USERS_CLEAN_FILE, 'users_append.parquet', DATA_CLEAN_DIR, 'users_append'),
    ]:
        session_files = sorted(glob_module.glob(str(output_dir / pattern)))
        if not session_files:
            continue

        logger.info(f"Found {len(session_files)} {file_type} files")

        # Collect all files (main file + session files)
        all_files = []
        if main_file.exists():
            all_files.append(str(main_file))
        all_files.extend(session_files)

        if len(all_files) <= 1:
            # If there is only one file and it is a session file, rename it to the main file
            if session_files and not main_file.exists():
                import shutil
                shutil.move(session_files[0], str(main_file))
                logger.info(f"Moved {session_files[0]} -> {main_file}")
            continue

        # Merge all files.
        logger.info(f"Merging {len(all_files)} files into {main_file.name}...")
        tables = [pq.read_table(f) for f in all_files]
        combined = pa.concat_tables(tables)
        pq.write_table(combined, main_file, compression='snappy')
        logger.info(f"Merge complete, total {combined.num_rows:,} rows")

        # Delete session files
        for sf in session_files:
            Path(sf).unlink()
            logger.info(f"Deleted {sf}")

        del tables, combined
        gc.collect()

    logger.info("All session files merged!")


def main():
    parser = argparse.ArgumentParser(description='Polymarket data tools')
    parser.add_argument('-v', '--verbose', action='store_true')
    subparsers = parser.add_subparsers(dest='command')

    # fetch-onchain
    p1 = subparsers.add_parser('fetch-onchain', help='Fetch on-chain data')
    p1.add_argument('-b', '--blocks', type=int, help='Most recent N blocks')
    p1.add_argument('-r', '--range', nargs=2, type=int, metavar=('START', 'END'))
    p1.add_argument('-c', '--continue', dest='continue_from', action='store_true')
    p1.add_argument('-a', '--alchemy', action='store_true')
    p1.add_argument('-m', '--merge', action='store_true',
                    help='Merge temporary files into the main files after completion (default: do not merge)')

    # fetch-markets
    p2 = subparsers.add_parser('fetch-markets', help='Incrementally fetch new markets')
    p2.add_argument('-c', '--continue', dest='continue_from', action='store_true',
                    help='Continue fetching from the last checkpoint')

    # update-markets
    p2b = subparsers.add_parser('update-markets', help='Update markets that are not resolved')
    p2b.add_argument('-c', '--continue', dest='continue_from', action='store_true',
                     help='Continue updating from the last checkpoint')

    # process
    p3 = subparsers.add_parser('process', help='Process trade data (small datasets)')
    p3.add_argument('--skip-missing', action='store_true', help='Skip missing token backfill')

    # process-historical
    p_hist = subparsers.add_parser('process-historical', help='Process large historical files in batches')
    p_hist.add_argument('-b', '--batch-size', type=int, default=1000000,
                        help='Rows processed per batch (default: 1,000,000)')
    p_hist.add_argument('-c', '--continue', dest='continue_from', action='store_true',
                        help='Continue processing from the last checkpoint')
    p_hist.add_argument('--test-batches', type=int, default=None,
                        help='Test mode: process only the first N batches')

    # clean-users
    p4 = subparsers.add_parser('clean-users', help='Clean user data')
    p4.add_argument('-b', '--batch-size', type=int, default=5_000_000, help='Batch size')
    p4.add_argument('-t', '--test', type=int, default=None, help='Test mode: process only the first N rows')

    # clean-trades
    p5 = subparsers.add_parser('clean-trades', help='Clean trade data (for quant use)')
    p5.add_argument('-b', '--batch-size', type=int, default=5_000_000, help='Batch size')
    p5.add_argument('-t', '--test', type=int, default=None, help='Test mode: process only the first N rows')

    # clean (both)
    p6 = subparsers.add_parser('clean', help='Run all data cleaning')
    p6.add_argument('-b', '--batch-size', type=int, default=5_000_000, help='Batch size')
    p6.add_argument('-t', '--test', type=int, default=None, help='Test mode: process only the first N rows')

    # update
    p7 = subparsers.add_parser('update', help='Full update')
    p7.add_argument('-a', '--alchemy', action='store_true')
    p7.add_argument('--skip-missing', action='store_true', help='Skip missing token backfill')
    p7.add_argument('--clean', dest='with_clean', action='store_true', help='Also run data cleaning')

    # merge-sessions
    p8 = subparsers.add_parser('merge-sessions', help='Merge all session files into the main files')

    args = parser.parse_args()
    setup_logging(args.verbose)

    if args.command == 'fetch-onchain':
        cmd_fetch_onchain(args)
    elif args.command == 'fetch-markets':
        cmd_fetch_markets(args)
    elif args.command == 'update-markets':
        cmd_update_markets(args)
    elif args.command == 'process':
        cmd_process(args)
    elif args.command == 'process-historical':
        cmd_process_historical(args)
    elif args.command == 'clean-users':
        cmd_clean_users(args)
    elif args.command == 'clean-trades':
        cmd_clean_trades(args)
    elif args.command == 'clean':
        cmd_clean(args)
    elif args.command == 'update':
        cmd_update(args)
    elif args.command == 'merge-sessions':
        cmd_merge_sessions(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
