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
CTF_POSITIONS_FILE = DATASET_DIR / 'ctf_positions.parquet'

# CSV preview files (stored in data/latest_result/)
MARKETS_PREVIEW_FILE = LATEST_RESULT_DIR / 'markets.csv'
ORDERFILLED_PREVIEW_FILE = LATEST_RESULT_DIR / 'orderfilled.csv'
TRADES_PREVIEW_FILE = LATEST_RESULT_DIR / 'trades.csv'
RESOLUTIONS_PREVIEW_FILE = LATEST_RESULT_DIR / 'resolutions.csv'
CTF_POSITIONS_PREVIEW_FILE = LATEST_RESULT_DIR / 'ctf_positions.csv'

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

# ERC-1155 + CTF position-change events on the same CTF contract.
# Verified against on-chain logs.
# keccak256("TransferSingle(address,address,address,uint256,uint256)")
TRANSFER_SINGLE_TOPIC = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
# keccak256("TransferBatch(address,address,address,uint256[],uint256[])")
TRANSFER_BATCH_TOPIC = '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb'
# keccak256("PositionSplit(address,address,bytes32,bytes32,uint256[],uint256)")
POSITION_SPLIT_TOPIC = '0x2e6bb91f8cbcda0c93623c54d0403a43514fabc40084ec96b6d5379a74786298'
# keccak256("PositionsMerge(address,address,bytes32,bytes32,uint256[],uint256)")
POSITIONS_MERGE_TOPIC = '0x6f13ca62553fcc2bcd2372180a43949c1e4cebba603901ede2f4e14f36b282ca'
# keccak256("PayoutRedemption(address,address,bytes32,bytes32,uint256[],uint256)")
PAYOUT_REDEMPTION_TOPIC = '0x2682012a4a4f1973119f1c9b90745d1bd91fa2bab387344f044cb3586864d18d'

# All event signatures the project knows how to decode.
EVENT_SIGNATURES = {
    'OrderFilled': 'd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6',
    'ConditionPreparation': 'ab3760c3bd2bb38b5bcf54dc79802ed67338b4cf29f3054ded67ed24661e4177',
    'ConditionResolution': 'b44d84d3289691f71497564b85d4233648d9dbae8cbdbb4329f301c3a0185894',
    'TransferSingle': 'c3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62',
    'TransferBatch': '4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb',
    'PositionSplit': '2e6bb91f8cbcda0c93623c54d0403a43514fabc40084ec96b6d5379a74786298',
    'PositionsMerge': '6f13ca62553fcc2bcd2372180a43949c1e4cebba603901ede2f4e14f36b282ca',
    'PayoutRedemption': '2682012a4a4f1973119f1c9b90745d1bd91fa2bab387344f044cb3586864d18d',
}


def get_event_name(signature: str) -> str:
    """Get the event name from a signature."""
    sig = signature.replace('0x', '').lower()
    for name, s in EVENT_SIGNATURES.items():
        if s.lower() == sig:
            return name
    return 'Unknown'
