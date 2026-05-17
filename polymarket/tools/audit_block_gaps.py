#!/usr/bin/env python3
"""
审计 orderfilled 数据集中缺失的区块。

背景（issue #1）：早期 RPC 抓取器在连续断连时会跳过整批 100 个区块，
所以已经发布的 parquet 文件里散布着完整缺失的区块段。本工具：

1. 扫描一个或多个 orderfilled*.parquet 文件，构造已抓取到事件的区块集合
2. 在期望区间内找出 >= --gap-threshold 个连续没有任何事件的段（候选 gap）
3. 对每段候选 gap 直接查链：如果链上 *确实* 有 OrderFilled 事件而我们没有，
   就把这段写入 failed_blocks_audit_<timestamp>.txt
4. 之后用 refetch_failed_blocks.py 补爬即可

用法:
    # 审计单个或多个 parquet（自动取并集），区间从数据本身推断
    python -m polymarket.tools.audit_block_gaps data/dataset/orderfilled.parquet

    # 指定显式区间和 gap 阈值
    python -m polymarket.tools.audit_block_gaps data/dataset/orderfilled_part*.parquet \\
        --start 80000000 --end 81000000 --gap-threshold 20

    # 只输出报告，不真正调用 RPC 验证
    python -m polymarket.tools.audit_block_gaps data/dataset/orderfilled.parquet --dry-run
"""
import argparse
import glob
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import pyarrow.parquet as pq

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from polymarket.fetchers.rpc import LogFetcher
from polymarket.config import DATA_DIR

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def collect_seen_blocks(parquet_paths: List[Path]) -> set:
    """从多个 parquet 文件读取 block_number 列，返回已见到事件的区块集合。"""
    seen = set()
    for p in parquet_paths:
        logger.info(f"读取 {p.name} ...")
        table = pq.read_table(p, columns=['block_number'])
        col = table.column('block_number').to_pylist()
        before = len(seen)
        seen.update(int(b) for b in col if b is not None)
        logger.info(f"  +{len(seen) - before:,} 新增唯一区块，累计 {len(seen):,}")
    return seen


def find_gaps(seen: set, start: int, end: int, threshold: int) -> List[Tuple[int, int]]:
    """在 [start, end] 闭区间内找出 >= threshold 个连续没有事件的段。"""
    gaps = []
    run_start = None
    for b in range(start, end + 1):
        if b not in seen:
            if run_start is None:
                run_start = b
        else:
            if run_start is not None:
                run_len = b - run_start
                if run_len >= threshold:
                    gaps.append((run_start, b - 1))
                run_start = None
    if run_start is not None:
        run_len = end + 1 - run_start
        if run_len >= threshold:
            gaps.append((run_start, end))
    return gaps


def verify_gap_against_chain(fetcher: LogFetcher, start: int, end: int) -> Tuple[bool, int]:
    """查询链上该区间是否真的有 OrderFilled 事件。

    返回 (真的缺数据, 链上事件数)。
    """
    logs = fetcher.client.get_logs(start, end)
    if logs is None:
        # RPC 仍然失败，保守起见也当成 gap 让 refetch 重试
        logger.warning(f"  区块 {start}-{end} 链上查询失败，保守标记为缺失")
        return True, -1
    return len(logs) > 0, len(logs)


def main():
    parser = argparse.ArgumentParser(description='审计 orderfilled 数据集中缺失的区块')
    parser.add_argument('parquet_paths', nargs='+',
                        help='一个或多个 orderfilled*.parquet 文件路径（支持 glob）')
    parser.add_argument('--start', type=int, default=None,
                        help='审计区间起始区块，默认取数据中的最小 block_number')
    parser.add_argument('--end', type=int, default=None,
                        help='审计区间结束区块，默认取数据中的最大 block_number')
    parser.add_argument('--gap-threshold', type=int, default=20,
                        help='连续多少个空白区块算作可疑 gap（默认 20）。'
                             'issue #1 中的真实 gap 一次 100 个区块。')
    parser.add_argument('--dry-run', action='store_true',
                        help='只列出候选 gap，不查链验证')
    parser.add_argument('--output', type=str, default=None,
                        help='验证过的缺失区块输出文件，默认 data/failed_blocks_audit_<ts>.txt')
    parser.add_argument('--alchemy', action='store_true',
                        help='使用 ALCHEMY_API_KEY 环境变量做 RPC（更高速率）')
    args = parser.parse_args()

    # 展开 glob
    paths: List[Path] = []
    for pattern in args.parquet_paths:
        matched = [Path(p) for p in glob.glob(pattern)]
        if not matched:
            p = Path(pattern)
            if p.exists():
                matched = [p]
        if not matched:
            logger.warning(f"未匹配到任何文件: {pattern}")
        paths.extend(matched)

    if not paths:
        logger.error("没有找到任何 parquet 文件")
        sys.exit(1)

    seen = collect_seen_blocks(paths)
    if not seen:
        logger.error("没有读取到任何 block_number")
        sys.exit(1)

    data_min = min(seen)
    data_max = max(seen)
    start = args.start if args.start is not None else data_min
    end = args.end if args.end is not None else data_max
    logger.info(f"数据覆盖区间: [{data_min:,}, {data_max:,}]  唯一区块 {len(seen):,}")
    logger.info(f"审计区间:     [{start:,}, {end:,}]")
    logger.info(f"gap 阈值:     {args.gap_threshold}")

    gaps = find_gaps(seen, start, end, args.gap_threshold)
    if not gaps:
        logger.info("未发现任何可疑 gap")
        return
    logger.info(f"发现 {len(gaps)} 个候选 gap")

    total_missing_blocks = sum(e - s + 1 for s, e in gaps)
    logger.info(f"候选 gap 总区块数: {total_missing_blocks:,}")

    if args.dry_run:
        for s, e in gaps[:50]:
            logger.info(f"  gap {s:,} - {e:,}  ({e - s + 1} 个区块)")
        if len(gaps) > 50:
            logger.info(f"  ... 共 {len(gaps)} 个 gap，省略剩余 {len(gaps) - 50} 个")
        return

    # 验证每个 gap
    fetcher = LogFetcher(use_alchemy=args.alchemy)
    output = Path(args.output) if args.output else (
        DATA_DIR / f"failed_blocks_audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )
    output.parent.mkdir(parents=True, exist_ok=True)

    confirmed = 0
    confirmed_events = 0
    benign = 0
    with open(output, 'w') as f:
        for idx, (s, e) in enumerate(gaps, 1):
            really_missing, n_events = verify_gap_against_chain(fetcher, s, e)
            if really_missing:
                f.write(f"{s}-{e}\n")
                f.flush()
                confirmed += 1
                if n_events > 0:
                    confirmed_events += n_events
                marker = f"链上 {n_events} 个事件" if n_events >= 0 else "RPC 失败，保守标记"
                logger.info(f"[{idx}/{len(gaps)}] ✗ {s:,}-{e:,} 确认缺失 ({marker})")
            else:
                benign += 1
                logger.info(f"[{idx}/{len(gaps)}] ✓ {s:,}-{e:,} 链上确认无事件")

    logger.info("")
    logger.info(f"审计完成: 确认缺失 {confirmed} 段（共 ~{confirmed_events:,} 条事件），"
                f"虚假告警 {benign} 段")
    if confirmed > 0:
        logger.info(f"输出文件: {output}")
        logger.info(f"补爬命令: python -m polymarket.tools.refetch_failed_blocks {output}")


if __name__ == '__main__':
    main()
