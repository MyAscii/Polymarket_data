"""Polymarket On-Chain Data Toolkit

A high-performance toolkit for fetching and processing Polymarket on-chain data
directly from Polygon blockchain.
"""

__version__ = "1.0.0"

from polymarket.fetchers.rpc import LogFetcher, PolygonRpcClient
from polymarket.fetchers.gamma import GammaApiClient
from polymarket.fetchers.resolutions import ResolutionFetcher
from polymarket.processors.decoder import EventDecoder
from polymarket.processors.resolution_decoder import ResolutionDecoder
from polymarket.processors.trades import extract_trades, load_token_mapping, find_missing_tokens
from polymarket.processors.cleaner import clean_trades_df, clean_users_df

__all__ = [
    "LogFetcher",
    "PolygonRpcClient",
    "GammaApiClient",
    "ResolutionFetcher",
    "EventDecoder",
    "ResolutionDecoder",
    "extract_trades",
    "load_token_mapping",
    "find_missing_tokens",
    "clean_trades_df",
    "clean_users_df",
]
