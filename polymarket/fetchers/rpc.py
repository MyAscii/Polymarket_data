"""
Polygon RPC client and log fetching.
"""

import logging
import time
from datetime import datetime
from typing import Dict, List, Any, Optional

from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

from ..config import (
    POLYGON_RPC_URL, get_rpc_url, BLOCKS_PER_BATCH, REQUEST_DELAY,
    POLYMARKET_CONTRACTS, EVENT_SIGNATURES, ORDER_FILLED_TOPICS,
)

logger = logging.getLogger(__name__)


class PolygonRpcClient:
    """Polygon RPC client."""

    # Polygon block time is about 2 seconds.
    BLOCK_TIME = 2

    def __init__(self, use_alchemy: bool = False):
        rpc_url = get_rpc_url(use_alchemy)
        self.w3 = Web3(Web3.HTTPProvider(rpc_url))
        # Polygon is a POA chain, so middleware is required for the extraData field.
        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        # web3.py requires checksum address formatting.
        self.contract_addresses = [Web3.to_checksum_address(addr) for addr in POLYMARKET_CONTRACTS.values()]
        self._timestamp_cache: Dict[int, int] = {}
        logger.info(f"RPC connection: {rpc_url.split('/v2/')[0] if '/v2/' in rpc_url else rpc_url}")

    def get_latest_block(self) -> int:
        return self.w3.eth.block_number

    def get_logs(self, start_block: int, end_block: int,
                 max_retries: int = 3, retry_backoff: float = 1.5) -> Optional[List[Dict[str, Any]]]:
        """Get OrderFilled logs within a block range.

        Retries transient RPC errors with exponential backoff before giving up.

        Returns:
            List[Dict]: log list on success, which may be empty
            None: returned only when all retries are exhausted
        """
        last_error = None
        for attempt in range(max_retries):
            try:
                logs = self.w3.eth.get_logs({
                    'fromBlock': start_block,
                    'toBlock': end_block,
                    'address': self.contract_addresses,
                    # topic0 in {V1, V2} — covers both OrderFilled formats so the
                    # same crawler keeps producing rows across the V2 cutover.
                    'topics': [ORDER_FILLED_TOPICS],
                })
                return [dict(log) for log in logs]
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    sleep_for = retry_backoff ** attempt
                    logger.warning(
                        f"get_logs({start_block}-{end_block}) attempt {attempt + 1}/{max_retries} "
                        f"failed: {e}; retrying in {sleep_for:.1f}s"
                    )
                    time.sleep(sleep_for)
        logger.error(f"get_logs({start_block}-{end_block}) failed after {max_retries} attempts: {last_error}")
        return None  # None means request failure, distinct from an empty list for no data.

    def get_block_timestamp(self, block_number: int) -> int:
        """Get a block timestamp."""
        if block_number in self._timestamp_cache:
            return self._timestamp_cache[block_number]
        try:
            block = self.w3.eth.get_block(block_number)
            ts = block['timestamp']
            self._timestamp_cache[block_number] = ts
            return ts
        except Exception as e:
            # Do not return an incorrect timestamp; let the caller handle the error.
            raise RuntimeError(f"Unable to get the timestamp for block {block_number}: {e}")

    def batch_get_timestamps(self, block_numbers: List[int]) -> Dict[int, int]:
        """Get timestamps in batch."""
        result = {}
        for bn in block_numbers:
            result[bn] = self.get_block_timestamp(bn)
        return result

    def estimate_timestamps(self, block_numbers: List[int]) -> Dict[int, int]:
        """Estimate timestamps to reduce RPC calls."""
        if not block_numbers:
            return {}

        sorted_blocks = sorted(block_numbers)
        first_ts = self.get_block_timestamp(sorted_blocks[0])

        result = {}
        for bn in sorted_blocks:
            offset = (bn - sorted_blocks[0]) * self.BLOCK_TIME
            result[bn] = first_ts + offset
        return result

    def test_connection(self) -> bool:
        try:
            self.w3.eth.block_number
            return True
        except Exception:
            return False


class LogFetcher:
    """On-chain log fetcher."""

    def __init__(self, use_alchemy: bool = False):
        self.client = PolygonRpcClient(use_alchemy=use_alchemy)
        self.address_to_name = {
            addr.lower(): name for name, addr in POLYMARKET_CONTRACTS.items()
        }

    def fetch_block_range(self, start_block: int, end_block: int) -> Optional[List[Dict[str, Any]]]:
        """Get logs for the specified block range.

        Returns:
            List[Dict]: record list on success, which may be empty
            None: returned when the RPC request fails
        """
        logger.info(f"Fetching blocks {start_block} - {end_block}")

        logs = self.client.get_logs(start_block, end_block)
        if logs is None:
            return None  # RPC failure.
        if not logs:
            return []  # Success, but no data.

        # Get timestamps, preferring blockTimestamp returned by the RPC.
        block_timestamps = {}
        unique_blocks_without_ts = set()

        for log in logs:
            bn = log['blockNumber']
            if isinstance(bn, str):
                bn = int(bn, 16) if bn.startswith('0x') else int(bn)

            # Check whether the RPC returned blockTimestamp.
            block_ts = log.get('blockTimestamp')
            if block_ts:
                if isinstance(block_ts, str):
                    ts = int(block_ts, 16) if block_ts.startswith('0x') else int(block_ts)
                else:
                    ts = int(block_ts)
                block_timestamps[bn] = ts
            else:
                unique_blocks_without_ts.add(bn)

        # Query only the blocks missing timestamps.
        if unique_blocks_without_ts:
            missing_timestamps = (
                self.client.batch_get_timestamps(sorted(unique_blocks_without_ts))
                if len(unique_blocks_without_ts) <= 3
                else self.client.estimate_timestamps(sorted(unique_blocks_without_ts))
            )
            block_timestamps.update(missing_timestamps)

        # Process logs.
        records = []
        for log in logs:
            record = self._process_log(log, start_block, end_block, block_timestamps)
            if record:
                records.append(record)

        logger.info(f"Fetched {len(records)} records")
        return records

    def _process_log(self, log: Dict, start_block: int, end_block: int,
                     block_timestamps: Dict[int, int]) -> Optional[Dict[str, Any]]:
        """Process a single log."""
        try:
            log_address = log.get('address', '').lower()
            contract_name = self.address_to_name.get(log_address, 'Unknown')

            bn = log['blockNumber']
            if isinstance(bn, str):
                bn = int(bn, 16) if bn.startswith('0x') else int(bn)

            # Get the block timestamp; if it is missing, warn and fetch it separately.
            timestamp = block_timestamps.get(bn)
            if timestamp is None:
                logger.warning(f"Block {bn} is missing a timestamp, trying to fetch it separately...")
                try:
                    timestamp = self._get_block_timestamp(bn)
                    block_timestamps[bn] = timestamp
                except Exception as e:
                    logger.error(f"Unable to get the timestamp for block {bn}: {e}")
                    # Skip this record instead of using an incorrect timestamp.
                    return None

            tx_hash = log['transactionHash']
            if hasattr(tx_hash, 'hex'):
                tx_hash = tx_hash.hex()

            topics = [t.hex() if hasattr(t, 'hex') else t for t in log['topics']]

            # Identify the event name.
            event_name = 'Unknown'
            event_sig = ''
            if topics:
                event_sig = topics[0].replace('0x', '').lower()
                for name, sig in EVENT_SIGNATURES.items():
                    if sig.lower() == event_sig:
                        event_name = name
                        break

            return {
                'contract': contract_name,
                'address': log['address'],
                'block_number': bn,
                'transaction_hash': tx_hash,
                'log_index': log['logIndex'],
                'timestamp': timestamp,
                'block_range': f"{start_block}-{end_block}",
                'topics': topics,
                'data': log['data'],
                'event_name': event_name,
                'event_signature': event_sig
            }
        except Exception as e:
            logger.warning(f"Failed to process log: {e}")
            return None

    def fetch_range_in_batches(self, start_block: int, end_block: int,
                                batch_size: int = BLOCKS_PER_BATCH,
                                bisect_on_failure: bool = True) -> Optional[List[Dict[str, Any]]]:
        """Fetch in batches.

        If a batch fails and bisect_on_failure is True, the batch is split in
        half and each half is retried independently, recursing down to a single
        block. This prevents one transient failure from dropping an entire
        100-block range — the historical cause of silent block gaps in the
        dataset.

        Returns:
            List[Dict]: record list on success, which may be empty
            None: returned only when even single-block fetches fail
        """
        all_records = []
        current = start_block

        while current <= end_block:
            batch_end = min(current + batch_size - 1, end_block)
            records = self.fetch_block_range(current, batch_end)
            if records is None:
                if bisect_on_failure and batch_end > current:
                    # Split the failing range and retry each half. Recursing
                    # narrows the failure to the actual bad block(s), letting
                    # the surrounding good blocks succeed.
                    mid = (current + batch_end) // 2
                    logger.warning(
                        f"Bisecting failed range {current}-{batch_end} -> "
                        f"{current}-{mid}, {mid + 1}-{batch_end}"
                    )
                    left = self.fetch_range_in_batches(current, mid, batch_size, bisect_on_failure)
                    right = self.fetch_range_in_batches(mid + 1, batch_end, batch_size, bisect_on_failure)
                    if left is None or right is None:
                        # One of the sub-ranges still couldn't be fetched.
                        return None
                    all_records.extend(left)
                    all_records.extend(right)
                else:
                    # Single block still failing, or bisection disabled.
                    return None
            else:
                all_records.extend(records)
            current = batch_end + 1
            if current <= end_block:
                time.sleep(REQUEST_DELAY)

        logger.info(f"Fetched a total of {len(all_records)} records")
        return all_records

    def get_latest_block(self) -> int:
        return self.client.get_latest_block()

    def test_connection(self) -> bool:
        return self.client.test_connection()
