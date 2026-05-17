"""
CTF resolution log fetcher.

Fetches ConditionPreparation and ConditionResolution events from the
Conditional Token Framework contract on Polygon. These events give
ground-truth market creation and final payout vectors — the only
authoritative source for who won each Polymarket market.
"""
import logging
import time
from typing import Any, Dict, List, Optional

from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

from ..config import (
    BLOCKS_PER_BATCH,
    CONDITION_PREPARATION_TOPIC,
    CONDITION_RESOLUTION_TOPIC,
    CTF_CONTRACT_ADDRESS,
    REQUEST_DELAY,
    get_rpc_url,
)

logger = logging.getLogger(__name__)

CTF_TOPICS = [CONDITION_PREPARATION_TOPIC, CONDITION_RESOLUTION_TOPIC]


class ResolutionFetcher:
    """Fetch CTF ConditionPreparation + ConditionResolution events.

    The implementation mirrors LogFetcher's retry-and-bisect strategy so
    transient RPC errors do not silently drop block ranges. This is the same
    correctness fix applied to the OrderFilled crawler.
    """

    def __init__(self, use_alchemy: bool = False, rpc_url: Optional[str] = None):
        url = rpc_url or get_rpc_url(use_alchemy)
        self.w3 = Web3(Web3.HTTPProvider(url, request_kwargs={'timeout': 30}))
        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        self.ctf_address = Web3.to_checksum_address(CTF_CONTRACT_ADDRESS)
        self._timestamp_cache: Dict[int, int] = {}
        logger.info(f"ResolutionFetcher RPC: {url.split('/v2/')[0] if '/v2/' in url else url}")

    def get_latest_block(self) -> int:
        return self.w3.eth.block_number

    def test_connection(self) -> bool:
        try:
            self.w3.eth.block_number
            return True
        except Exception:
            return False

    def _get_logs(self, start_block: int, end_block: int,
                  max_retries: int = 3, retry_backoff: float = 1.5
                  ) -> Optional[List[Dict[str, Any]]]:
        """Fetch raw logs for the CTF contract with retry + exponential backoff."""
        last_error = None
        # topics=[CTF_TOPICS] means "topic0 in CTF_TOPICS" — i.e., either event.
        for attempt in range(max_retries):
            try:
                logs = self.w3.eth.get_logs({
                    'fromBlock': start_block,
                    'toBlock': end_block,
                    'address': self.ctf_address,
                    'topics': [CTF_TOPICS],
                })
                return [dict(log) for log in logs]
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    sleep_for = retry_backoff ** attempt
                    logger.warning(
                        f"CTF get_logs({start_block}-{end_block}) attempt "
                        f"{attempt + 1}/{max_retries} failed: {e}; retry in {sleep_for:.1f}s"
                    )
                    time.sleep(sleep_for)
        logger.error(
            f"CTF get_logs({start_block}-{end_block}) failed after {max_retries} attempts: {last_error}"
        )
        return None

    def _get_block_timestamp(self, block_number: int) -> int:
        if block_number in self._timestamp_cache:
            return self._timestamp_cache[block_number]
        block = self.w3.eth.get_block(block_number)
        ts = block['timestamp']
        self._timestamp_cache[block_number] = ts
        return ts

    def fetch_block_range(self, start_block: int, end_block: int
                          ) -> Optional[List[Dict[str, Any]]]:
        """Fetch CTF logs in the given block range.

        Returns:
            list of dicts on success (possibly empty)
            None on persistent RPC failure
        """
        logger.info(f"Fetching CTF resolutions for blocks {start_block} - {end_block}")
        logs = self._get_logs(start_block, end_block)
        if logs is None:
            return None
        if not logs:
            return []

        # Attach timestamps. Prefer blockTimestamp from the RPC when present.
        block_timestamps: Dict[int, int] = {}
        missing: set = set()
        for log in logs:
            bn = log['blockNumber']
            if isinstance(bn, str):
                bn = int(bn, 16) if bn.startswith('0x') else int(bn)
            ts = log.get('blockTimestamp')
            if ts is not None:
                if isinstance(ts, str):
                    ts = int(ts, 16) if ts.startswith('0x') else int(ts)
                block_timestamps[bn] = int(ts)
            else:
                missing.add(bn)

        for bn in sorted(missing):
            try:
                block_timestamps[bn] = self._get_block_timestamp(bn)
            except Exception as e:
                logger.warning(f"Could not fetch timestamp for block {bn}: {e}")

        records = []
        for log in logs:
            bn = log['blockNumber']
            if isinstance(bn, str):
                bn = int(bn, 16) if bn.startswith('0x') else int(bn)
            tx_hash = log['transactionHash']
            if hasattr(tx_hash, 'hex'):
                h = tx_hash.hex()
                tx_hash = h if h.startswith('0x') else '0x' + h
            topics = []
            for t in log['topics']:
                if hasattr(t, 'hex'):
                    th = t.hex()
                    topics.append(th if th.startswith('0x') else '0x' + th)
                else:
                    topics.append(t)
            data = log['data']
            if hasattr(data, 'hex'):
                d = data.hex()
                data = d if d.startswith('0x') else '0x' + d
            records.append({
                'block_number': bn,
                'transaction_hash': tx_hash,
                'log_index': log['logIndex'],
                'timestamp': block_timestamps.get(bn, 0),
                'address': log.get('address', ''),
                'topics': topics,
                'data': data,
            })

        logger.info(f"Fetched {len(records)} CTF events")
        return records

    def fetch_range_in_batches(self, start_block: int, end_block: int,
                                batch_size: int = BLOCKS_PER_BATCH,
                                bisect_on_failure: bool = True
                                ) -> Optional[List[Dict[str, Any]]]:
        """Fetch a range in batches, bisecting any batch that fails."""
        all_records: List[Dict[str, Any]] = []
        current = start_block
        while current <= end_block:
            batch_end = min(current + batch_size - 1, end_block)
            records = self.fetch_block_range(current, batch_end)
            if records is None:
                if bisect_on_failure and batch_end > current:
                    mid = (current + batch_end) // 2
                    logger.warning(
                        f"Bisecting failed CTF range {current}-{batch_end} -> "
                        f"{current}-{mid}, {mid + 1}-{batch_end}"
                    )
                    left = self.fetch_range_in_batches(current, mid, batch_size, bisect_on_failure)
                    right = self.fetch_range_in_batches(mid + 1, batch_end, batch_size, bisect_on_failure)
                    if left is None or right is None:
                        return None
                    all_records.extend(left)
                    all_records.extend(right)
                else:
                    return None
            else:
                all_records.extend(records)
            current = batch_end + 1
            if current <= end_block:
                time.sleep(REQUEST_DELAY)
        logger.info(f"Fetched a total of {len(all_records)} CTF events")
        return all_records
