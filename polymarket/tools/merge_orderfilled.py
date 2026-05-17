#!/usr/bin/env python3
"""
Script specifically for merging orderfilled files.

Handles schema mismatch issues:
- Normalize data types to string
- Normalize column order
- Handle missing columns
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


def convert_batch_to_target_schema(batch, target_schema):
    """
    Convert a batch to the target schema.

    Args:
        batch: RecordBatch
        target_schema: target schema

    Returns:
        Converted RecordBatch
    """
    import pandas as pd

    # Convert to pandas.
    df = batch.to_pandas()

    # Create a new DataFrame following the target schema.
    new_df = pd.DataFrame()

    for field in target_schema:
        col_name = field.name

        if col_name in df.columns:
            # Column exists, convert its type.
            if pa.types.is_string(field.type):
                # Convert to string.
                new_df[col_name] = df[col_name].astype(str)
            elif pa.types.is_integer(field.type):
                # Convert to integer.
                new_df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0).astype('int64')
            else:
                new_df[col_name] = df[col_name]
        else:
            # Column does not exist, create an empty column.
            if pa.types.is_string(field.type):
                new_df[col_name] = ''
            elif pa.types.is_integer(field.type):
                new_df[col_name] = 0
            else:
                new_df[col_name] = None

    # Convert back to an Arrow Table.
    table = pa.Table.from_pandas(new_df, schema=target_schema)
    return table.to_batches()[0]


def merge_orderfilled_files(file1, file2, output_file, auto_yes=False):
    """
    Merge two orderfilled files.

    Args:
        file1: first file (schema conversion required)
        file2: second file (used as the schema baseline)
        output_file: output file
        auto_yes: automatically confirm overwrite
    """
    logger.info("=== Checking input files ===")

    # Check files.
    pf1 = pq.ParquetFile(file1)
    pf2 = pq.ParquetFile(file2)

    logger.info(f"[1] {Path(file1).name}: {pf1.metadata.num_rows:,} rows")
    logger.info(f"[2] {Path(file2).name}: {pf2.metadata.num_rows:,} rows")
    logger.info(f"Total: {pf1.metadata.num_rows + pf2.metadata.num_rows:,} rows")

    # Get the target schema from the second file.
    target_schema = pf2.schema_arrow
    logger.info("\nTarget schema (based on file 2):")
    for field in target_schema:
        logger.info(f"  {field.name}: {field.type}")

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

    # Start merging.
    logger.info("\n=== Starting merge ===")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        writer = pq.ParquetWriter(output_file, target_schema, compression='snappy')
        total_rows_written = 0
        batch_size = 500000

        # Process the first file and convert its schema.
        logger.info(f"[1/2] Processing {Path(file1).name} (converting schema)")
        batch_iter1 = pf1.iter_batches(batch_size=batch_size)
        batch_num = 1

        for batch in batch_iter1:
            # Convert schema.
            converted_batch = convert_batch_to_target_schema(batch, target_schema)
            writer.write_batch(converted_batch)
            total_rows_written += len(converted_batch)
            if batch_num % 10 == 0:
                logger.info(f"  Wrote batch {batch_num}: {len(converted_batch):,} rows (total {total_rows_written:,} rows)")
            batch_num += 1

        logger.info(f"  ✓ File 1 complete, total {pf1.metadata.num_rows:,} rows")

        # Process the second file directly.
        logger.info(f"[2/2] Processing {Path(file2).name}")
        batch_iter2 = pf2.iter_batches(batch_size=batch_size)
        batch_num = 1

        for batch in batch_iter2:
            writer.write_batch(batch)
            total_rows_written += len(batch)
            if batch_num % 10 == 0:
                logger.info(f"  Wrote batch {batch_num}: {len(batch):,} rows (total {total_rows_written:,} rows)")
            batch_num += 1

        logger.info(f"  ✓ File 2 complete, total {pf2.metadata.num_rows:,} rows")

        # Close writer.
        writer.close()

        output_size_mb = output_path.stat().st_size / 1024 / 1024
        logger.info("\n=== Merge complete ===")
        logger.info(f"Output file: {output_file}")
        logger.info(f"Total rows: {total_rows_written:,}")
        logger.info(f"File size: {output_size_mb:.1f} MB")

        return True

    except Exception as e:
        logger.error(f"Merge failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Merge orderfilled files (handle schema mismatches)'
    )

    parser.add_argument('file1', help='First file (requires conversion)')
    parser.add_argument('file2', help='Second file (used as the schema baseline)')
    parser.add_argument('-o', '--output', required=True, help='Output file path')
    parser.add_argument('-y', '--yes', action='store_true', help='Automatically confirm overwrite')

    args = parser.parse_args()

    success = merge_orderfilled_files(args.file1, args.file2, args.output, args.yes)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
