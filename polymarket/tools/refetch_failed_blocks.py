#!/usr/bin/env python3
"""
补爬失败区块的脚本

从 failed_blocks 文件读取失败的区块范围，逐个补爬并保存到独立的 parquet 文件
"""
import os
import sys
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# 现在可以导入了
from polymarket.fetchers.rpc import LogFetcher
from polymarket.processors import (
    EventDecoder,
    extract_trades,
    load_token_mapping,
    clean_trades_df,
    clean_users_df
)
from polymarket.config import MARKETS_FILE, MISSING_MARKETS_FILE
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def read_failed_blocks(failed_blocks_file):
    """读取失败区块列表"""
    blocks = []
    with open(failed_blocks_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and '-' in line:
                start, end = line.split('-')
                blocks.append((int(start), int(end)))
    return blocks


def main():
    if len(sys.argv) < 2:
        print("用法: python scripts/refetch_failed_blocks.py <failed_blocks_file>")
        print("示例: python scripts/refetch_failed_blocks.py data/failed_blocks_20251230_055516.txt")
        sys.exit(1)

    failed_blocks_file = Path(sys.argv[1])
    if not failed_blocks_file.exists():
        logger.error(f"文件不存在: {failed_blocks_file}")
        sys.exit(1)

    # 读取失败区块列表
    failed_ranges = read_failed_blocks(failed_blocks_file)
    logger.info(f"读取到 {len(failed_ranges)} 个失败区块范围")

    # 初始化
    fetcher = LogFetcher()
    decoder = EventDecoder()

    # 加载 token 映射
    token_mapping = load_token_mapping(MARKETS_FILE)
    if MISSING_MARKETS_FILE.exists():
        token_mapping.update(load_token_mapping(MISSING_MARKETS_FILE))
    logger.info(f"加载 {len(token_mapping)} 个 token 映射")

    # 准备输出文件
    output_dir = project_root / 'data' / 'dataset'
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = failed_blocks_file.stem.replace('failed_blocks_', '')
    orderfilled_file = output_dir / f'orderfilled_refetched_{timestamp}.parquet'
    trades_file = output_dir / f'trades_refetched_{timestamp}.parquet'

    data_clean_dir = project_root / 'data' / 'data_clean'
    data_clean_dir.mkdir(parents=True, exist_ok=True)
    quant_file = data_clean_dir / f'quant_refetched_{timestamp}.parquet'
    users_file = data_clean_dir / f'users_refetched_{timestamp}.parquet'

    # 收集所有数据
    all_events = []
    all_trades = []
    all_quant = []
    all_users = []

    failed_count = 0
    success_count = 0

    # 关键修复（issue #1）：原版补爬器如果二次 RPC 失败，只是 logger.error
    # 然后丢弃这些区块，导致数据集永久缺失。这里改为把失败的范围写入
    # 一个新的 failed_blocks 文件，用户可以多次迭代直到收敛。
    remaining_file = failed_blocks_file.with_name(
        failed_blocks_file.stem.replace('failed_blocks_', 'failed_blocks_remaining_') + '.txt'
    )

    def record_remaining(block_start, block_end):
        with open(remaining_file, 'a') as fh:
            fh.write(f"{block_start}-{block_end}\n")

    # 逐个补爬
    for idx, (start, end) in enumerate(failed_ranges, 1):
        logger.info(f"[{idx}/{len(failed_ranges)}] 补爬区块 {start}-{end}")

        logs = fetcher.fetch_range_in_batches(start, end)

        if logs is None:
            logger.error(f"  ✗ RPC 请求失败 — 记录到 {remaining_file.name}")
            record_remaining(start, end)
            failed_count += 1
            continue

        if not logs:
            logger.info(f"  ✓ 无交易数据")
            success_count += 1
            continue

        # 解码
        decoded = decoder.decode_batch(logs)
        formatted = decoder.format_batch(decoded)

        if not formatted:
            logger.info(f"  ✓ 解码后无有效数据")
            success_count += 1
            continue

        # 添加到列表
        all_events.extend(formatted)

        # 生成 trades
        trades_df = extract_trades(formatted, token_mapping)
        if not trades_df.empty:
            all_trades.append(trades_df)

            # 生成 quant
            quant_df = clean_trades_df(trades_df)
            if not quant_df.empty:
                all_quant.append(quant_df)

            # 生成 users
            users_df = clean_users_df(trades_df)
            if not users_df.empty:
                all_users.append(users_df)

        logger.info(f"  ✓ 获取 {len(formatted)} 条事件")
        success_count += 1

    # 保存数据
    logger.info(f"\n补爬完成: 成功 {success_count}, 失败 {failed_count}")

    if all_events:
        logger.info(f"保存 {len(all_events)} 条 orderfilled 事件...")
        events_df = pd.DataFrame(all_events)
        events_df.to_parquet(orderfilled_file, index=False, compression='snappy')
        logger.info(f"  ✓ 已保存到: {orderfilled_file}")

    if all_trades:
        logger.info(f"保存 trades 数据...")
        trades_combined = pd.concat(all_trades, ignore_index=True)
        trades_combined.to_parquet(trades_file, index=False, compression='snappy')
        logger.info(f"  ✓ 已保存 {len(trades_combined)} 条 trades 到: {trades_file}")

    if all_quant:
        logger.info(f"保存 quant 数据...")
        quant_combined = pd.concat(all_quant, ignore_index=True)
        quant_combined.to_parquet(quant_file, index=False, compression='snappy')
        logger.info(f"  ✓ 已保存 {len(quant_combined)} 条 quant 到: {quant_file}")

    if all_users:
        logger.info(f"保存 users 数据...")
        users_combined = pd.concat(all_users, ignore_index=True)
        users_combined.to_parquet(users_file, index=False, compression='snappy')
        logger.info(f"  ✓ 已保存 {len(users_combined)} 条 users 到: {users_file}")

    if failed_count > 0:
        logger.warning(f"⚠️ 仍有 {failed_count} 个区块范围未恢复，已写入: {remaining_file}")
        logger.warning(f"   可以再次运行: python -m polymarket.tools.refetch_failed_blocks {remaining_file}")

    logger.info(f"\n全部完成!")


if __name__ == '__main__':
    main()
