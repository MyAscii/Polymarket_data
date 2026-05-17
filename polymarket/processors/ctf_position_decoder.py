"""
Decoder for CTF position-change events.

Output schema (one row per position change — TransferBatch is exploded into
one row per id within the batch):

    block_number, transaction_hash, log_index, sub_index, timestamp, datetime,
    event_name,
    operator, from_address, to_address,    # populated for transfers
    actor,                                  # populated for split/merge/redemption
    collateral_token, parent_collection_id, condition_id, index_sets,
    position_id, amount

uint256 values (position_id, amount) are stored as decimal strings because
they overflow int64.
"""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from eth_abi import decode as abi_decode
from web3 import Web3

from ..config import (
    PAYOUT_REDEMPTION_TOPIC,
    POSITION_SPLIT_TOPIC,
    POSITIONS_MERGE_TOPIC,
    TRANSFER_BATCH_TOPIC,
    TRANSFER_SINGLE_TOPIC,
)

logger = logging.getLogger(__name__)


def _norm(t: str) -> str:
    return t.lower() if t.startswith('0x') else '0x' + t.lower()


def _addr_from_topic(topic: str) -> str:
    raw = topic.replace('0x', '')
    return Web3.to_checksum_address('0x' + raw[-40:])


def _bytes32_from_topic(topic: str) -> str:
    return topic if topic.startswith('0x') else '0x' + topic


def _data_bytes(raw: Any) -> bytes:
    if isinstance(raw, bytes):
        return raw
    if not raw:
        return b''
    return bytes.fromhex(raw.replace('0x', ''))


class CtfPositionDecoder:
    """Decode CTF TransferSingle/Batch + PositionSplit/Merge/PayoutRedemption."""

    TS = _norm(TRANSFER_SINGLE_TOPIC)
    TB = _norm(TRANSFER_BATCH_TOPIC)
    SP = _norm(POSITION_SPLIT_TOPIC)
    MG = _norm(POSITIONS_MERGE_TOPIC)
    RD = _norm(PAYOUT_REDEMPTION_TOPIC)

    @classmethod
    def _base(cls, record: Dict[str, Any], event_name: str, sub_index: int = 0) -> Dict[str, Any]:
        ts = record.get('timestamp', 0)
        dt = ''
        if isinstance(ts, (int, float)) and 0 < ts < 4102444800:
            dt = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        return {
            'block_number': int(record.get('block_number', 0)),
            'transaction_hash': record.get('transaction_hash', ''),
            'log_index': int(record.get('log_index', 0)),
            'sub_index': int(sub_index),
            'timestamp': int(ts) if ts else 0,
            'datetime': dt,
            'event_name': event_name,
            'operator': '',
            'from_address': '',
            'to_address': '',
            'actor': '',
            'collateral_token': '',
            'parent_collection_id': '',
            'condition_id': '',
            'index_sets': [],
            'position_id': '',
            'amount': '0',
        }

    @classmethod
    def decode(cls, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Decode a raw log into one or more rows.

        TransferBatch is exploded: N ids in the batch -> N rows with sub_index
        0..N-1. All other event types produce exactly one row.

        Returns an empty list for logs whose topic[0] is unknown.
        """
        topics = record.get('topics') or []
        if not topics:
            return []
        t0 = _norm(topics[0])

        if t0 == cls.TS:
            return cls._decode_transfer_single(record, topics)
        if t0 == cls.TB:
            return cls._decode_transfer_batch(record, topics)
        if t0 == cls.SP:
            return cls._decode_split_or_merge(record, topics, 'PositionSplit')
        if t0 == cls.MG:
            return cls._decode_split_or_merge(record, topics, 'PositionsMerge')
        if t0 == cls.RD:
            return cls._decode_redemption(record, topics)
        return []

    @classmethod
    def decode_batch(cls, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for r in records:
            rows.extend(cls.decode(r))
        return rows

    # ---- per-event decoders --------------------------------------------------

    @classmethod
    def _decode_transfer_single(cls, record: Dict[str, Any], topics: List[str]) -> List[Dict[str, Any]]:
        if len(topics) < 4:
            return []
        try:
            position_id, amount = abi_decode(['uint256', 'uint256'], _data_bytes(record.get('data')))
        except Exception as e:
            logger.warning(f"TransferSingle ABI decode failed: {e}")
            return []
        row = cls._base(record, 'TransferSingle')
        row['operator'] = _addr_from_topic(topics[1])
        row['from_address'] = _addr_from_topic(topics[2])
        row['to_address'] = _addr_from_topic(topics[3])
        row['position_id'] = str(position_id)
        row['amount'] = str(amount)
        return [row]

    @classmethod
    def _decode_transfer_batch(cls, record: Dict[str, Any], topics: List[str]) -> List[Dict[str, Any]]:
        if len(topics) < 4:
            return []
        try:
            ids, values = abi_decode(['uint256[]', 'uint256[]'], _data_bytes(record.get('data')))
        except Exception as e:
            logger.warning(f"TransferBatch ABI decode failed: {e}")
            return []
        if len(ids) != len(values):
            logger.warning(
                f"TransferBatch ids/values length mismatch in tx "
                f"{record.get('transaction_hash')}: {len(ids)} vs {len(values)}"
            )
            return []
        operator = _addr_from_topic(topics[1])
        from_addr = _addr_from_topic(topics[2])
        to_addr = _addr_from_topic(topics[3])
        rows: List[Dict[str, Any]] = []
        for i, (pid, val) in enumerate(zip(ids, values)):
            row = cls._base(record, 'TransferBatch', sub_index=i)
            row['operator'] = operator
            row['from_address'] = from_addr
            row['to_address'] = to_addr
            row['position_id'] = str(pid)
            row['amount'] = str(val)
            rows.append(row)
        return rows

    @classmethod
    def _decode_split_or_merge(cls, record: Dict[str, Any], topics: List[str],
                                event_name: str) -> List[Dict[str, Any]]:
        # Indexed: stakeholder, parentCollectionId, conditionId.
        # Data:    collateralToken (address), partition (uint256[]), amount (uint256).
        if len(topics) < 4:
            return []
        try:
            collateral, partition, amount = abi_decode(
                ['address', 'uint256[]', 'uint256'], _data_bytes(record.get('data'))
            )
        except Exception as e:
            logger.warning(f"{event_name} ABI decode failed: {e}")
            return []
        row = cls._base(record, event_name)
        row['actor'] = _addr_from_topic(topics[1])
        row['parent_collection_id'] = _bytes32_from_topic(topics[2])
        row['condition_id'] = _bytes32_from_topic(topics[3])
        row['collateral_token'] = Web3.to_checksum_address(collateral)
        # uint256[] inside the int64 range: keep as ints (these are bitmask outcome indices).
        row['index_sets'] = [int(x) for x in partition]
        row['amount'] = str(amount)
        return [row]

    @classmethod
    def _decode_redemption(cls, record: Dict[str, Any], topics: List[str]) -> List[Dict[str, Any]]:
        # Indexed: redeemer, collateralToken, parentCollectionId.
        # Data:    conditionId (bytes32), indexSets (uint256[]), payout (uint256).
        if len(topics) < 4:
            return []
        try:
            condition_id_b, index_sets, payout = abi_decode(
                ['bytes32', 'uint256[]', 'uint256'], _data_bytes(record.get('data'))
            )
        except Exception as e:
            logger.warning(f"PayoutRedemption ABI decode failed: {e}")
            return []
        row = cls._base(record, 'PayoutRedemption')
        row['actor'] = _addr_from_topic(topics[1])
        row['collateral_token'] = _addr_from_topic(topics[2])
        row['parent_collection_id'] = _bytes32_from_topic(topics[3])
        h = condition_id_b.hex() if hasattr(condition_id_b, 'hex') else str(condition_id_b)
        row['condition_id'] = h if h.startswith('0x') else '0x' + h
        row['index_sets'] = [int(x) for x in index_sets]
        row['amount'] = str(payout)
        return [row]
