import faulthandler
import os
import signal
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import typer
from fetchjobs import get_all_jobs, get_jobs_from_db
from tqdm import tqdm

faulthandler.register(signal.SIGQUIT)

app = typer.Typer()

def yield_jobs_per_window(window=timedelta(hours=24), showpb=True):
    start_range, end_range = 1725608400, 1780351201
    start_range, end_range = (
        datetime.fromtimestamp(start_range, UTC),
        datetime.fromtimestamp(end_range, UTC),
    )
    start_window = start_range.replace(hour=0, minute=0, second=0, microsecond=0)
    windows = []
    while start_window < end_range:
        windows.append(start_window)
        start_window += window

    for day in tqdm(windows, disable=not showpb):
        start_window = day.timestamp()
        end_window = (day + window).timestamp()
        yield from get_jobs_from_db(start_window, end_window)

def flatten_dict(d, parent_key="", sep="__"):
    items = []
    for key, value in d.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten_dict(value, new_key, sep).items())
        else:
            items.append((new_key, value))
    return dict(items)

def chunker(gen, num):
    current_chunk = []
    for job in gen:
        current_chunk.append(job)

        if len(current_chunk) == num:
            yield current_chunk
            current_chunk = []

    if current_chunk:
        yield current_chunk

def compare_two_files(file1, file2):
    h1 = duckdb.sql(f"SELECT sum(hash(t)) FROM '{file1}' t").fetchone()[0]
    h2 = duckdb.sql(f"SELECT sum(hash(t)) FROM '{file2}' t").fetchone()[0]
    result = h1 == h2
    if not result:
        print(f"{file1} is not the same as {file2}")
    return result

class Mode(str, Enum):
    dump = "dump"
    count = "count"
    compare = "compare"

@app.command()
def run_main(
    batch_length: int | None = None,
    mode: Mode = Mode.dump
):
    export_folder = Path("../dbexports")

    if mode == Mode.count:
        for filename in export_folder.glob("*.parquet"):
            metadata = pq.read_metadata(filename)
            print(filename, metadata.num_rows)
    elif mode == Mode.compare:
        groups = []
        for filename in export_folder.glob("*.parquet"):
            for group in groups:
                if compare_two_files(filename, group[0]):
                    group.append(filename)
                    break
            else:
                groups.append([filename])

        print("Groups:")
        for group in groups:
            print(group)
    elif mode == Mode.dump:
        filename = export_folder

        if batch_length is None:
            print("Full grab")
            filename = filename / "all_jobs_full.parquet"
            job_generator = tqdm(get_all_jobs(), total=64209465)
        else:
            print(f"Per window grab ({batch_length} hours at a time)")
            filename = filename / f"all_jobs_perwindow{batch_length}.parquet"
            job_generator = yield_jobs_per_window(window=timedelta(hours=batch_length))

        # Adjust chunk size based on your RAM
        CHUNK_SIZE = 50000

        if os.path.isfile(filename):
            os.remove(filename)

        schema = pa.schema([
            pa.field("time", pa.struct([("start", pa.uint64()), ("end", pa.uint64()), ("elapsed", pa.uint64()), ("submit", pa.uint64()), ("total", pa.float64())])),
            pa.field("alloc", pa.struct([("cpu", pa.uint64()), ("mem", pa.uint64()), ("energy", pa.uint64()), ("node", pa.uint64()), ("billing", pa.uint64())])),
            pa.field("req", pa.struct([("cpu", pa.uint64()), ("mem", pa.uint64()), ("node", pa.uint64()), ("billing", pa.uint64())])),
            pa.field("state", pa.string()),
            pa.field("job_db_inx", pa.uint64()),
            pa.field("timelimit", pa.uint64()),
            pa.field("id_job", pa.uint64()),
        ])

        with pq.ParquetWriter(filename, schema, compression="snappy") as writer:
            for current_chunk in chunker(job_generator, CHUNK_SIZE):
                table = pa.Table.from_pylist(current_chunk, schema=schema)
                writer.write_table(table)

if __name__ == "__main__":
    app()
