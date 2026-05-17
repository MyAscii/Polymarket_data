"""
Trade data extraction.

Only OrderFilled events from the two exchange contracts are needed.
Supports market_id/condition_id mapping and filling missing tokens.

Based on the processing logic from poly_data-main:
1. Find the non-USDC asset_id (the non-"0" value in maker_asset_id or taker_asset_id)
2. Join the markets table using the non-USDC asset_id to get market_id, condition_id, and side (token1/token2)
3. Divide amounts by 10^6 (USDC has 6 decimals)
"""

import logging
from typing import Dict, List, Any, Optional, Set

import pandas as pd

from ..config import MARKETS_FILE

logger = logging.getLogger(__name__)

# USDC precision.
USDC_DECIMALS = 10 ** 6

# Names of the two exchange contracts.
EXCHANGE_CONTRACTS = {'CTF_EXCHANGE', 'NEGRISK_CTF_EXCHANGE'}


def extract_trades(events: List[Dict[str, Any]], token_mapping: Optional[Dict[str, Dict]] = None) -> pd.DataFrame:
    """
    Extract trade data from events.

    Keep only:
    - data from the two exchange contracts (CTF_EXCHANGE, NEGRISK_CTF_EXCHANGE)
    - OrderFilled events

    Args:
        events: list of decoded events
        token_mapping: token_id -> {market_id, condition_id, side, question} mapping
    """
    trades = []

    for event in events:
        # If event_name exists, verify that it is OrderFilled.
        # orderfilled.parquet may already be filtered and not include this field.
        event_name = event.get('event_name')
        if event_name is not None and event_name != 'OrderFilled':
            continue

        # Keep only the two exchange contracts.
        contract = event.get('contract', '')
        if contract not in EXCHANGE_CONTRACTS:
            continue

        # Parse the trade.
        trade = _parse_order_filled(event, token_mapping)
        if trade:
            trades.append(trade)

    logger.info(f"Extracted {len(trades)} trades")

    if not trades:
        return pd.DataFrame()

    return pd.DataFrame(trades)


def _parse_order_filled(event: Dict, token_mapping: Optional[Dict[str, Dict]] = None) -> Optional[Dict]:
    """Parse an OrderFilled event."""
    maker_id = str(event.get('maker_asset_id', '0'))
    taker_id = str(event.get('taker_asset_id', '0'))

    # Amounts may be strings and need conversion.
    try:
        maker_amt = float(event.get('maker_amount_filled', 0))
        taker_amt = float(event.get('taker_amount_filled', 0))
    except (ValueError, TypeError):
        maker_amt = 0
        taker_amt = 0

    # Find the non-USDC asset_id.
    # The USDC asset_id is "0".
    if maker_id != '0':
        nonusdc_asset_id = maker_id
        usdc_amt = taker_amt
        token_amt = maker_amt
        # maker provides token, taker provides USDC -> maker sells, taker buys
        maker_dir, taker_dir = 'SELL', 'BUY'
        maker_asset = 'token'  # Will be filled as token1/token2.
        taker_asset = 'USDC'
    elif taker_id != '0':
        nonusdc_asset_id = taker_id
        usdc_amt = maker_amt
        token_amt = taker_amt
        # maker provides USDC, taker provides token -> maker buys, taker sells
        maker_dir, taker_dir = 'BUY', 'SELL'
        maker_asset = 'USDC'
        taker_asset = 'token'  # Will be filled as token1/token2.
    else:
        # Both sides are 0, skip.
        return None

    # Divide amounts by 10^6 (USDC precision).
    usdc_amt_normalized = usdc_amt / USDC_DECIMALS
    token_amt_normalized = token_amt / USDC_DECIMALS

    # Compute price.
    price = usdc_amt_normalized / token_amt_normalized if token_amt_normalized > 0 else 0

    # Map market information.
    market_id = ''
    condition_id = ''
    side = ''  # token1 or token2
    question = ''
    event_id = ''
    event_slug = ''
    event_title = ''

    if token_mapping and nonusdc_asset_id:
        market_info = token_mapping.get(nonusdc_asset_id, {})
        market_id = str(market_info.get('market_id', ''))
        condition_id = str(market_info.get('condition_id', ''))
        side = str(market_info.get('side', ''))
        question = str(market_info.get('question', ''))
        event_id = str(market_info.get('event_id', ''))
        event_slug = str(market_info.get('event_slug', ''))
        event_title = str(market_info.get('event_title', ''))

        # Update asset labels to token1/token2.
        if side:
            if maker_asset == 'token':
                maker_asset = side
            if taker_asset == 'token':
                taker_asset = side

    return {
        'timestamp': event.get('timestamp'),
        'datetime': event.get('datetime', ''),
        'block_number': event.get('block_number'),
        'transaction_hash': event.get('transaction_hash'),
        'contract': event.get('contract', ''),
        # Event information.
        'event_id': event_id,
        'event_slug': event_slug,
        'event_title': event_title,
        # Market information.
        'market_id': market_id,
        'condition_id': condition_id,
        'question': question,
        'nonusdc_side': side,  # token1 or token2 (consistent with poly_data-main)
        # Trade participants.
        'maker': event.get('maker'),
        'taker': event.get('taker'),
        'maker_asset': maker_asset,
        'taker_asset': taker_asset,
        'maker_direction': maker_dir,
        'taker_direction': taker_dir,
        # Amounts (normalized, divided by 10^6).
        'price': round(price, 6),
        'usd_amount': round(usdc_amt_normalized, 2),
        'token_amount': round(token_amt_normalized, 2),
        # Original data.
        'asset_id': nonusdc_asset_id,
        'order_hash': event.get('order_hash', '')
    }


def load_token_mapping(markets_file=None) -> Dict[str, Dict]:
    """
    Load token mappings from the markets parquet file.

    Returns: token_id -> {market_id, condition_id, side, question}
    """
    if markets_file is None:
        markets_file = MARKETS_FILE

    if not markets_file.exists():
        logger.warning(f"Markets file does not exist: {markets_file}")
        return {}

    try:
        # Read with pyarrow to avoid pandas compatibility issues.
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(markets_file)
        df = pf.read().to_pandas()
        mapping = {}

        for _, row in df.iterrows():
            market_id = str(row.get('id', ''))
            condition_id = str(row.get('condition_id', ''))
            question = str(row.get('question', ''))[:100]  # Truncate overly long questions.
            token1 = str(row.get('token1', ''))
            token2 = str(row.get('token2', ''))
            # Event information.
            event_id = str(row.get('event_id', ''))
            event_slug = str(row.get('event_slug', ''))
            event_title = str(row.get('event_title', ''))[:100]

            if token1:
                mapping[token1] = {
                    'market_id': market_id,
                    'condition_id': condition_id,
                    'side': 'token1',
                    'question': question,
                    'event_id': event_id,
                    'event_slug': event_slug,
                    'event_title': event_title,
                }
            if token2:
                mapping[token2] = {
                    'market_id': market_id,
                    'condition_id': condition_id,
                    'side': 'token2',
                    'question': question,
                    'event_id': event_id,
                    'event_slug': event_slug,
                    'event_title': event_title,
                }

        logger.info(f"Loaded {len(mapping)} token mappings")
        return mapping
    except Exception as e:
        logger.error(f"Failed to load markets file: {e}")
        return {}


def find_missing_tokens(trades_df: pd.DataFrame, token_mapping: Dict[str, Dict]) -> Set[str]:
    """Find tokens in trades that are missing from the mapping."""
    if trades_df.empty or 'asset_id' not in trades_df.columns:
        return set()

    all_tokens = set(trades_df['asset_id'].dropna().astype(str).unique())
    all_tokens.discard('')
    all_tokens.discard('0')

    missing = all_tokens - set(token_mapping.keys())
    if missing:
        logger.info(f"Found {len(missing)} unmapped tokens")
    return missing


def save_preview_csv(trades_df: pd.DataFrame, output_file, n_rows: int = 1000):
    """Save the latest N trades as a CSV preview, keeping all fields."""
    if trades_df.empty:
        return

    # Keep all fields and only limit the number of rows.
    preview_df = trades_df.tail(n_rows)
    preview_df.to_csv(output_file, index=False)
    logger.info(f"Saved {len(preview_df)} trade preview rows to {output_file}")


# Preserve the old class interface for compatibility.
class TradeBuilder:
    """Trade builder compatible with the old interface."""

    def __init__(self, token_mapping: Optional[Dict[str, Dict]] = None):
        self.token_mapping = token_mapping

    def build_from_events(self, events: List[Dict]) -> List[Dict]:
        df = extract_trades(events, self.token_mapping)
        return df.to_dict('records') if not df.empty else []

    def to_dataframe(self, trades: List[Dict]) -> pd.DataFrame:
        return pd.DataFrame(trades) if trades else pd.DataFrame()


class TokenMapper:
    """Token mapper."""

    def __init__(self, markets_file=None):
        self.token_map = load_token_mapping(markets_file)

    def get_market(self, token_id: str) -> Optional[Dict]:
        return self.token_map.get(str(token_id))

    def add_markets(self, markets: List[Dict]):
        """Add new markets to the mapping."""
        for m in markets:
            market_id = str(m.get('id', ''))
            condition_id = str(m.get('condition_id', ''))
            question = str(m.get('question', ''))[:100]

            if m.get('token1'):
                self.token_map[m['token1']] = {
                    'market_id': market_id,
                    'condition_id': condition_id,
                    'side': 'token1',
                    'question': question
                }
            if m.get('token2'):
                self.token_map[m['token2']] = {
                    'market_id': market_id,
                    'condition_id': condition_id,
                    'side': 'token2',
                    'question': question
                }
