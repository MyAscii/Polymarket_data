"""
Decoder for CTF ConditionPreparation and ConditionResolution events.

ConditionPreparation marks a market being created on-chain.
ConditionResolution carries the final payout vector — the ground truth
for which outcome won (and the on-chain settlement of any redemption).
"""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from eth_abi import decode as abi_decode
from web3 import Web3

from ..config import (
    CONDITION_PREPARATION_TOPIC,
    CONDITION_RESOLUTION_TOPIC,
)

logger = logging.getLogger(__name__)


class ResolutionDecoder:
    """Decode CTF ConditionPreparation / ConditionResolution events."""

    PREP_TOPIC = CONDITION_PREPARATION_TOPIC.lower()
    RES_TOPIC = CONDITION_RESOLUTION_TOPIC.lower()

    @staticmethod
    def _norm_topic(t: str) -> str:
        return t.lower() if t.startswith('0x') else '0x' + t.lower()

    @classmethod
    def _topic_to_address(cls, topic: str) -> str:
        # bytes32-padded address; the address is the last 20 bytes.
        raw = topic.replace('0x', '')
        return Web3.to_checksum_address('0x' + raw[-40:])

    @classmethod
    def _topic_to_bytes32_hex(cls, topic: str) -> str:
        return topic if topic.startswith('0x') else '0x' + topic

    @classmethod
    def decode(cls, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Decode a single raw CTF log into a structured row.

        Returns None for logs whose topic[0] is not one of the two CTF events
        we care about — caller should filter Nones.
        """
        topics = record.get('topics') or []
        if len(topics) < 4:
            return None
        t0 = cls._norm_topic(topics[0])
        if t0 == cls.PREP_TOPIC:
            event_name = 'ConditionPreparation'
        elif t0 == cls.RES_TOPIC:
            event_name = 'ConditionResolution'
        else:
            return None

        condition_id = cls._topic_to_bytes32_hex(topics[1])
        oracle = cls._topic_to_address(topics[2])
        question_id = cls._topic_to_bytes32_hex(topics[3])

        data = record.get('data', '0x')
        if isinstance(data, bytes):
            data_bytes = data
        else:
            data_bytes = bytes.fromhex(data.replace('0x', '')) if data else b''

        outcome_slot_count = 0
        payout_numerators: List[int] = []
        try:
            if event_name == 'ConditionPreparation':
                (outcome_slot_count,) = abi_decode(['uint256'], data_bytes)
            else:
                outcome_slot_count, payouts = abi_decode(['uint256', 'uint256[]'], data_bytes)
                payout_numerators = list(payouts)
        except Exception as e:
            logger.warning(
                f"Failed to ABI-decode {event_name} data for tx "
                f"{record.get('transaction_hash')}: {e}"
            )

        ts = record.get('timestamp', 0)
        dt = ''
        if isinstance(ts, (int, float)) and 0 < ts < 4102444800:
            dt = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        # Convenience field: for resolved binary markets, which outcome won?
        # For multi-outcome markets, the full payout vector is preserved.
        outcome_index: Optional[int] = None
        if event_name == 'ConditionResolution' and payout_numerators:
            total = sum(payout_numerators)
            if total > 0 and payout_numerators.count(total) == 1:
                outcome_index = payout_numerators.index(total)

        return {
            'block_number': int(record.get('block_number', 0)),
            'transaction_hash': record.get('transaction_hash', ''),
            'log_index': int(record.get('log_index', 0)),
            'timestamp': int(ts) if ts else 0,
            'datetime': dt,
            'event_name': event_name,
            'condition_id': condition_id,
            'oracle': oracle,
            'question_id': question_id,
            'outcome_slot_count': int(outcome_slot_count),
            # Store as a list; pyarrow handles list<uint64> natively.
            # Empty list for ConditionPreparation events.
            'payout_numerators': payout_numerators,
            'outcome_index': outcome_index,
        }

    @classmethod
    def decode_batch(cls, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        decoded = []
        for r in records:
            row = cls.decode(r)
            if row is not None:
                decoded.append(row)
        return decoded
