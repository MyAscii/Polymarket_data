"""
Event decoder for OrderFilled (V1 and V2).

Both protocol versions produce the same output schema. V2's `tokenId`+`side`
is mapped back to V1's `makerAssetId`/`takerAssetId` so the downstream trades
pipeline keeps working unchanged. V2-specific fields (`builder`, `metadata`,
explicit `side`) are added as extra columns with empty defaults for V1 rows.

USDC is asset id "0" in the V1 convention; for V2, the non-token side is
synthesized as "0" so the same convention holds across versions.
"""

import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

from eth_abi import decode as abi_decode
from eth_utils import to_checksum_address
from web3 import Web3

from ..config import (
    EVENT_SIGNATURES,
    EXCHANGE_VERSION,
    ORDER_FILLED_TOPIC_V1,
    ORDER_FILLED_TOPIC_V2,
)

logger = logging.getLogger(__name__)


def _norm(t: str) -> str:
    return t.lower() if t.startswith('0x') else '0x' + t.lower()


class EventDecoder:
    """OrderFilled event decoder. Handles both V1 and V2 ABIs."""

    V1_TOPIC = _norm(ORDER_FILLED_TOPIC_V1)
    V2_TOPIC = _norm(ORDER_FILLED_TOPIC_V2)

    # V1: OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)
    # Indexed: orderHash, maker, taker
    # Data:    makerAssetId, takerAssetId, makerAmountFilled, takerAmountFilled, fee
    V1_DATA_TYPES = ['uint256', 'uint256', 'uint256', 'uint256', 'uint256']

    # V2: OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)
    # Indexed: orderHash, maker, taker
    # Data:    side(uint8), tokenId, makerAmountFilled, takerAmountFilled, fee, builder, metadata
    V2_DATA_TYPES = ['uint8', 'uint256', 'uint256', 'uint256', 'uint256', 'bytes32', 'bytes32']

    def __init__(self):
        self.w3 = Web3()

    # --- top-level API -------------------------------------------------------

    def decode(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Decode an OrderFilled event (V1 or V2) into a uniform structure."""
        topics = record.get('topics', [])
        data = record.get('data', '')

        if not topics:
            record['event_name'] = 'Unknown'
            record['decoded_params'] = {}
            return record

        t0 = _norm(topics[0])
        is_v2 = t0 == self.V2_TOPIC
        record['event_name'] = 'OrderFilled'  # canonical name for both versions
        record['exchange_version'] = 'v2' if is_v2 else 'v1'

        # Indexed params are the same in V1 and V2.
        params: Dict[str, Any] = {}
        if len(topics) >= 4:
            params['orderHash'] = topics[1]
            params['maker'] = self._addr_from_topic(topics[2])
            params['taker'] = self._addr_from_topic(topics[3])

        try:
            data_bytes = self._to_bytes(data)
            if is_v2:
                self._decode_v2_data(data_bytes, params)
            else:
                self._decode_v1_data(data_bytes, params)
        except Exception as e:
            logger.warning(
                f"OrderFilled ABI decode failed (version={record['exchange_version']}, "
                f"tx={record.get('transaction_hash')}): {e}"
            )

        record['decoded_params'] = params
        return record

    def decode_batch(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [self.decode(r) for r in records]

    def format_event(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Format a decoded OrderFilled event into the output schema."""
        params = record.get('decoded_params', {})
        version = record.get('exchange_version', '')

        result: Dict[str, Any] = {
            'transaction_hash': record.get('transaction_hash', ''),
            'block_number': record.get('block_number', 0),
            'log_index': record.get('log_index', 0),
            'timestamp': record.get('timestamp', 0),
            'contract': record.get('contract', ''),
            'exchange_version': version or self._version_for_address(record.get('address', '')),
            'event_name': 'OrderFilled',
        }

        ts = result['timestamp']
        if isinstance(ts, (int, float)) and 0 < ts < 4102444800:
            result['datetime'] = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        result.update({
            'order_hash': params.get('orderHash', ''),
            'maker': params.get('maker', ''),
            'taker': params.get('taker', ''),
            # asset_id can be a huge uint256 — stringify to avoid int64 overflow.
            'maker_asset_id': str(params.get('makerAssetId', 0)),
            'taker_asset_id': str(params.get('takerAssetId', 0)),
            'maker_amount_filled': params.get('makerAmountFilled', 0),
            'taker_amount_filled': params.get('takerAmountFilled', 0),
            # Backward-compat: maker_fee receives the single on-chain fee value.
            # taker_fee/protocol_fee always 0 — neither V1 nor V2 emit those
            # (the previous decoder's separate fee columns were a latent bug
            # reading garbage padding bytes).
            'maker_fee': params.get('fee', 0),
            'taker_fee': 0,
            'protocol_fee': 0,
            'fee': params.get('fee', 0),
            # V2-only fields; '' / '' for V1 rows.
            'side': params.get('side', ''),
            'builder': params.get('builder', ''),
            'metadata': params.get('metadata', ''),
        })

        return result

    def format_batch(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [self.format_event(r) for r in records]

    # --- internals -----------------------------------------------------------

    @staticmethod
    def _addr_from_topic(topic: str) -> str:
        raw = topic.replace('0x', '') if isinstance(topic, str) else topic.hex()
        return to_checksum_address('0x' + raw[-40:])

    @staticmethod
    def _to_bytes(data: Any) -> bytes:
        if isinstance(data, bytes):
            return data
        if not data:
            return b''
        return bytes.fromhex(data.replace('0x', ''))

    def _decode_v1_data(self, data: bytes, params: Dict[str, Any]) -> None:
        if not data:
            return
        maker_id, taker_id, maker_amt, taker_amt, fee = abi_decode(self.V1_DATA_TYPES, data)
        params['makerAssetId'] = maker_id
        params['takerAssetId'] = taker_id
        params['makerAmountFilled'] = maker_amt
        params['takerAmountFilled'] = taker_amt
        params['fee'] = fee
        # Derive `side` for V1 to keep the column populated in both versions.
        # V1 convention: USDC asset id is 0. If maker pays token (makerAssetId != 0)
        # the maker is selling; otherwise buying.
        params['side'] = 'SELL' if maker_id != 0 else ('BUY' if taker_id != 0 else '')
        params['builder'] = ''
        params['metadata'] = ''

    def _decode_v2_data(self, data: bytes, params: Dict[str, Any]) -> None:
        if not data:
            return
        side, token_id, maker_amt, taker_amt, fee, builder, metadata = abi_decode(
            self.V2_DATA_TYPES, data
        )
        # Map V2 side+tokenId back onto V1's makerAssetId/takerAssetId so the
        # existing trades extractor (which finds the non-USDC asset id) works
        # unchanged. side=0 (BUY): maker pays USDC, gets tokenId.
        #                side=1 (SELL): maker pays tokenId, gets USDC.
        if int(side) == 0:
            params['makerAssetId'] = 0
            params['takerAssetId'] = token_id
            params['side'] = 'BUY'
        else:
            params['makerAssetId'] = token_id
            params['takerAssetId'] = 0
            params['side'] = 'SELL'
        params['makerAmountFilled'] = maker_amt
        params['takerAmountFilled'] = taker_amt
        params['fee'] = fee
        params['builder'] = '0x' + builder.hex() if isinstance(builder, (bytes, bytearray)) else str(builder)
        params['metadata'] = '0x' + metadata.hex() if isinstance(metadata, (bytes, bytearray)) else str(metadata)

    @staticmethod
    def _version_for_address(address: Any) -> str:
        if not address:
            return ''
        return EXCHANGE_VERSION.get(str(address).lower(), '')
