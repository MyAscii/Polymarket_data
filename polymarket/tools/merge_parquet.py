#!/usr/bin/env python3
"""
Merge multiple parquet files.

nohup python scripts/merge_parquet.py \
  data/dataset/orderfilled.parquet \
  data/dataset/orderfilled_refetched_20251230_055516.parquet \
  -o data/dataset/orderfilled_merged.parquet \
  -y \
  > logs/merge_orderfilled.log 2>&1 &

Supports specifying input files and an output file, merged in order.
"""
import sys
import argparse
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def merge_parquet_files(input_files, output_file, dry_run=False, auto_yes=False):
    """
    Merge multiple parquet files.

    Args:
        input_files: list of input files, in order
        output_file: output file path
        dry_run: show information only without performing the merge
        auto_yes: automatically confirm overwrite
    """
    # Validate input files.
    valid_files = []
    total_rows = 0

    logger.info("=== Checking input files ===")
    for i, file_path in enumerate(input_files, 1):
        p = Path(file_path)
        if not p.exists():
            logger.error(f"[{i}] File does not exist: {file_path}")
            continue

        try:
            # Read metadata only without loading actual data.
            parquet_file = pq.ParquetFile(file_path)
            rows = parquet_file.metadata.num_rows
            size_mb = p.stat().st_size / 1024 / 1024
            total_rows += rows
            valid_files.append(file_path)
            logger.info(f"[{i}] {p.name}: {rows:,} rows, {size_mb:.1f} MB")
        except Exception as e:
            logger.error(f"[{i}] Read failed for {file_path}: {e}")
            continue

    if not valid_files:
        logger.error("No valid input files")
        return False

    logger.info(f"\nTotal: {len(valid_files)} files, {total_rows:,} rows")

    if dry_run:
        logger.info(f"\n[DRY RUN] Would write to: {output_file}")
        return True

    # Check the output file.
    output_path = Path(output_file)
    if output_path.exists():
        logger.warning(f"Output file already exists and will be overwritten: {output_file}")
        if not auto_yes:
            response = input("Confirm overwrite? (yes/no): ")
            if response.lower() not in ['yes', 'y']:
                logger.info("Operation cancelled")
                return False
        else:
            logger.info("Overwrite confirmed automatically (--yes)")

    # Merge files with streaming writes and batched reads.
    logger.info("\n=== Starting merge (streaming writes + batched reads) ===")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        writer = None
        total_rows_written = 0
        batch_size = 500000  # Read 500k rows per batch to reduce memory usage.

        for i, file_path in enumerate(valid_files, 1):
            logger.info(f"[{i}/{len(valid_files)}] Processing {Path(file_path).name}")

            # Open the parquet file.
            parquet_file = pq.ParquetFile(file_path)
            file_rows = 0

            # Create the writer the first time after reading the first batch schema.
            if writer is None:
                # Create an iterator.
                batch_iter = parquet_file.iter_batches(batch_size=batch_size)
                # Read the first batch to get the schema.
                first_batch = next(batch_iter)
                target_schema = first_batch.schema
                writer = pq.ParquetWriter(output_file, target_schema, compression='snappy')
                writer.write_batch(first_batch)
                file_rows += len(first_batch)
                total_rows_written += len(first_batch)
                logger.info(f"  Wrote batch 1: {len(first_batch):,} rows")

                # Continue processing the remaining batches with the same iterator.
                batch_num = 2
                for batch in batch_iter:
                    writer.write_batch(batch)
                    file_rows += len(batch)
                    total_rows_written += len(batch)
                    logger.info(f"  Wrote batch {batch_num}: {len(batch):,} rows (total {total_rows_written:,} rows)")
                    batch_num += 1
            else:
                # Later files: normalize schema and then write in batches.
                batch_num = 1
                for batch in parquet_file.iter_batches(batch_size=batch_size):
                    # Convert the batch to the target schema.
                    table = pa.Table.from_batches([batch])
                    # Try converting to the target schema automatically.
                    try:
                        table = table.cast(target_schema)
                    except Exception as e:
                        # If automatic conversion fails, convert with pandas.
                        import pandas as pd
                        df = table.to_pandas()
                        # Convert types.
                        for field in target_schema:
                            if field.name in df.columns:
                                if pa.types.is_string(field.type):
                                    df[field.name] = df[field.name].astype(str)
                                elif pa.types.is_integer(field.type):
                                    df[field.name] = pd.to_numeric(df[field.name], errors='coerce').fillna(0).astype('int64')
                        table = pa.Table.from_pandas(df, schema=target_schema)

                    batch = table.to_batches()[0]
                    writer.write_batch(batch)
                    file_rows += len(batch)
                    total_rows_written += len(batch)
                    if batch_num % 10 == 0 or len(batch) < batch_size:  # Every 10 batches or the last batch.
                        logger.info(f"  Wrote batch {batch_num}: {len(batch):,} rows (total {total_rows_written:,} rows)")
                    batch_num += 1

            logger.info(f"  ✓ File complete, total {file_rows:,} rows")

        # Close writer.
        if writer:
            writer.close()

        output_size_mb = output_path.stat().st_size / 1024 / 1024
        logger.info("\n=== Merge complete ===")
        logger.info(f"Output file: {output_file}")
        logger.info(f"Total rows: {total_rows_written:,}")
        logger.info(f"File size: {output_size_mb:.1f} MB")

        return True

    except Exception as e:
        logger.error(f"Merge failed: {e}")
        if writer:
            writer.close()
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Merge multiple parquet files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Merge three files
  python scripts/merge_parquet.py \\
    data/dataset/orderfilled.parquet \\
    data/dataset/orderfilled_session_*.parquet \\
    data/dataset/orderfilled_refetched_*.parquet \\
    -o data/dataset/orderfilled_merged.parquet

  # Preview information without performing the merge
  python scripts/merge_parquet.py file1.parquet file2.parquet -o output.parquet --dry-run
        """
    )

    parser.add_argument(
        'input_files',
        nargs='+',
        help='Input parquet files (merged in order)'
    )

    parser.add_argument(
        '-o', '--output',
        required=True,
        help='Output file path'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show information only without merging'
    )

    parser.add_argument(
        '--log-file',
        help='Log file path (for nohup runs)'
    )

    parser.add_argument(
        '-y', '--yes',
        action='store_true',
        help='Automatically confirm overwrite without prompting (for nohup runs)'
    )

    args = parser.parse_args()

    # Reconfigure logging if a log file is specified.
    if args.log_file:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.FileHandler(args.log_file),
                logging.StreamHandler()  # Also output to the terminal.
            ],
            force=True
        )

    # Expand glob patterns.
    import glob
    input_files = []
    for pattern in args.input_files:
        matched = glob.glob(pattern)
        if matched:
            input_files.extend(sorted(matched))
        else:
            # Not a pattern, add directly.
            input_files.append(pattern)

    if not input_files:
        logger.error("No input files")
        sys.exit(1)

    success = merge_parquet_files(input_files, args.output, args.dry_run, args.yes)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
