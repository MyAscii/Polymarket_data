"""
poly_onchain configuration
"""

import os
from pathlib import Path
from typing import Optional


# ============== Paths ==============

# PROJECT_ROOT points to the poly_onchain directory itself
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / 'data'
LOG_DIR = PROJECT_ROOT / 'logs'

# Dataset directory (full parquet data)
DATASET_DIR = DATA_DIR / 'dataset'
# Latest results directory (CSV preview data)
LATEST_RESULT_DIR = DATA_DIR / 'latest_result'

# Full parquet data files (stored in data/dataset/)
DECODED_EVENTS_FILE = DATASET_DIR / 'orderfilled.parquet'
MARKETS_FILE = DATASET_DIR / 'markets.parquet'
MISSING_MARKETS_FILE = DATASET_DIR / 'missing_markets.parquet'
TRADES_OUTPUT_FILE = DATASET_DIR / 'trades.parquet'
RESOLUTIONS_FILE = DATASET_DIR / 'resolutions.parquet'

# CSV preview files (stored in data/latest_result/)
MARKETS_PREVIEW_FILE = LATEST_RESULT_DIR / 'markets.csv'
ORDERFILLED_PREVIEW_FILE = LATEST_RESULT_DIR / 'orderfilled.csv'
TRADES_PREVIEW_FILE = LATEST_RESULT_DIR / 'trades.csv'
RESOLUTIONS_PREVIEW_FILE = LATEST_RESULT_DIR / 'resolutions.csv'

# Cleaned data directory (stored in data/data_clean/)
DATA_CLEAN_DIR = DATA_DIR / 'data_clean'
USERS_CLEAN_FILE = DATA_CLEAN_DIR / 'users.parquet'
QUANT_CLEAN_FILE = DATA_CLEAN_DIR / 'quant.parquet'

# CSV previews for cleaned data (stored in data/latest_result/)
USERS_PREVIEW_FILE = LATEST_RESULT_DIR / 'users.csv'
QUANT_PREVIEW_FILE = LATEST_RESULT_DIR / 'quant.csv'

# State files
STATE_FILE = DATA_DIR / 'state.json'
TEMP_DIR = DATA_DIR / 'temp'


# ============== Blockchain ==============

POLYGON_CHAIN_ID = 137
POLYGON_RPC_URL = 'https://polygon-rpc.com'


def get_rpc_url(use_alchemy: bool = False) -> str:
    """Get the RPC URL."""
    if use_alchemy:
        api_key = os.getenv('ALCHEMY_API_KEY', '')
        if api_key:
            return f'https://polygon-mainnet.g.alchemy.com/v2/{api_key}'
    return POLYGON_RPC_URL


# ============== API ==============

GAMMA_API_URL = "https://gamma-api.polymarket.com"


# ============== Processing parameters ==============

BLOCKS_PER_BATCH = 100
REQUEST_DELAY = 0.2
USDC_ASSET_ID = '0'

OUTPUT_COLUMNS = [
    'timestamp', 'block_number', 'transactionHash', 'market_id',
    'maker', 'taker', 'nonusdc_side', 'maker_direction', 'taker_direction',
    'price', 'usd_amount', 'token_amount',
    'maker_fee', 'taker_fee', 'protocol_fee', 'order_hash'
]


# ============== Contract addresses ==============

# Default exchange contract addresses
_DEFAULT_CTF_EXCHANGE = '0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E'
_DEFAULT_NEGRISK_CTF_EXCHANGE = '0xC5d563A36AE78145C45a50134d48A1215220f80a'

# Read from environment variables to support customization
_CTF_EXCHANGE = os.getenv('POLYMARKET_CTF_EXCHANGE', _DEFAULT_CTF_EXCHANGE)
_NEGRISK_CTF_EXCHANGE = os.getenv('POLYMARKET_NEGRISK_CTF_EXCHANGE', _DEFAULT_NEGRISK_CTF_EXCHANGE)

# Listen only to the two exchange contracts (sources of OrderFilled events)
POLYMARKET_CONTRACTS = {
    'CTF_EXCHANGE': _CTF_EXCHANGE,
    'NEGRISK_CTF_EXCHANGE': _NEGRISK_CTF_EXCHANGE,
}

# Set of exchange addresses (lowercase, used for filtering)
EXCHANGE_ADDRESSES = {
    _CTF_EXCHANGE.lower(),
    _NEGRISK_CTF_EXCHANGE.lower()
}


# ============== Event signatures ==============

# OrderFilled event signature (with 0x prefix)
ORDER_FILLED_TOPIC = '0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6'

# Conditional Token Framework (CTF) on Polygon — market creation + resolution.
# Verified against on-chain logs at the CTF contract.
CTF_CONTRACT_ADDRESS = '0x4D97DCd97eC945f40cF65F87097ACe5EA0476045'
# keccak256("ConditionPreparation(bytes32,address,bytes32,uint256)")
CONDITION_PREPARATION_TOPIC = '0xab3760c3bd2bb38b5bcf54dc79802ed67338b4cf29f3054ded67ed24661e4177'
# keccak256("ConditionResolution(bytes32,address,bytes32,uint256,uint256[])")
CONDITION_RESOLUTION_TOPIC = '0xb44d84d3289691f71497564b85d4233648d9dbae8cbdbb4329f301c3a0185894'

# All event signatures the project knows how to decode.
EVENT_SIGNATURES = {
    'OrderFilled': 'd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6',
    'ConditionPreparation': 'ab3760c3bd2bb38b5bcf54dc79802ed67338b4cf29f3054ded67ed24661e4177',
    'ConditionResolution': 'b44d84d3289691f71497564b85d4233648d9dbae8cbdbb4329f301c3a0185894',
}


def get_event_name(signature: str) -> str:
    """Get the event name from a signature."""
    sig = signature.replace('0x', '').lower()
    for name, s in EVENT_SIGNATURES.items():
        if s.lower() == sig:
            return name
    return 'Unknown'
