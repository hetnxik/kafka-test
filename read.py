"""Stream events CSV in chunks."""
import pandas as pd
import config


def read_events(file_path, chunk_size=None):
    """Stream events CSV in chunks.

    Args:
        file_path: Path to the events CSV file
        chunk_size: Number of rows per chunk (defaults to config.CHUNK_SIZE)

    Yields:
        DataFrame chunks
    """
    chunk_size = chunk_size or config.CHUNK_SIZE

    # Parse timestamp columns
    parse_dates = ['timestamp', 'created_at']

    for chunk in pd.read_csv(
        file_path,
        chunksize=chunk_size,
        parse_dates=parse_dates,
        dtype={'event_payload': str, 'metadata': str}
    ):
        yield chunk
