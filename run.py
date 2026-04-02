"""Entry point - run the ETL pipeline."""
import argparse
import os
import sys
from datetime import datetime
import pandas as pd
import config
from read import read_events
from aggregate import Aggregator


def log(msg):
    """Print message with timestamp."""
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {msg}")
    sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser(description='Process events CSV to hourly aggregates')
    parser.add_argument('--file', required=True, help='Path to events CSV file')
    args = parser.parse_args()

    # Ensure output directory exists
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)

    log(f"Processing {args.file}...")

    aggregator = Aggregator(
        event_package_mapping=config.EVENT_PACKAGE_MAPPING,
    )

    # Process chunks
    total_rows = 0
    for i, chunk in enumerate(read_events(args.file)):
        chunk_rows = len(chunk)
        total_rows += chunk_rows
        log(f"  Chunk {i+1}: {chunk_rows} rows (total: {total_rows})")
        aggregator.process_chunk(chunk)

    log(f"Total rows processed: {total_rows}")

    # Convert to DataFrames
    hourly_df, trans_df = aggregator.to_dataframes()

    hourly_path = os.path.join(config.OUTPUT_DIR, 'hourly_event_counts.csv')
    trans_path = os.path.join(config.OUTPUT_DIR, 'event_transitions.csv')

    # Merge into existing output if it exists
    if os.path.exists(hourly_path):
        existing = pd.read_csv(hourly_path)
        hourly_df = pd.concat([existing, hourly_df], ignore_index=True)
        hourly_df = hourly_df.groupby(['date', 'hour_of_day', 'event_name', 'package_name', 'os']).agg(
            event_count=('event_count', 'sum'),
            session_count=('session_count', 'sum'),
            user_count=('user_count', 'sum'),
        ).reset_index().sort_values(['date', 'hour_of_day', 'event_name', 'package_name', 'os'])
        log(f"Merged with existing hourly output")

    if os.path.exists(trans_path):
        existing = pd.read_csv(trans_path)
        trans_df = pd.concat([existing, trans_df], ignore_index=True)
        trans_df = trans_df.groupby(['date', 'hour_of_day', 'from_event', 'to_event']).agg(
            transition_count=('transition_count', 'sum'),
        ).reset_index().sort_values(['date', 'hour_of_day', 'from_event', 'to_event'])
        log(f"Merged with existing transitions output")

    hourly_df.to_csv(hourly_path, index=False)
    log(f"Wrote {len(hourly_df)} rows to {hourly_path}")

    trans_df.to_csv(trans_path, index=False)
    log(f"Wrote {len(trans_df)} rows to {trans_path}")

    log("Done!")


if __name__ == '__main__':
    main()
