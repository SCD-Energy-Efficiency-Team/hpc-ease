try:
    import mariadb
except ImportError as e:
    pass
    # print(f"WARNING: Failed to import MariaDB, may cause crash if required... ({e})")

import math
from collections import defaultdict

import pyarrow.compute as pc
import pyarrow.dataset as ds
from dotenv import dotenv_values

STATES = [
    "PENDING", # 0
    "RUNNING", # 1
    "SUSPENDED", # 2
    "COMPLETED", # 3
    "CANCELLED", # 4
    "FAILED", # 5
    "TIMEOUT", # 6
    None,
    None,
    None,
    None,
    "NODE_FAIL", # 11
]

TRES_KEYS = {
    1: "cpu",
    2: "mem",
    3: "energy",
    4: "node",
    5: "billing",
    6: "disk",
    7: "vmem",
    8: "pages",
    1001: "gpu",
    1002: "gpumem",
    1003: "gpuutil",
    1004: "gpu:a100",
    1005: "gpu:a100_1g.10gb",
    1006: "gpu:1g.10gb"
}

def job_row_to_dict(row):
    time_dict = {}
    time_dict["start"] = row.pop("time_start")
    time_dict["end"] = row.pop("time_end")
    time_dict["elapsed"] = time_dict["end"] - time_dict["start"]
    time_dict["submit"] = row.pop("time_submit")
    time_dict["total"] = float(row.pop("total_cpu_time"))
    row_dict = {"time": time_dict, "alloc": {}, "req": {}}
    for line in row["tres_alloc"].split(","):
        key, value = tuple(map(int, line.split("=")))
        row_dict["alloc"][TRES_KEYS[key]] = value
    for line in row["tres_req"].split(","):
        key, value = tuple(map(int, line.split("=")))
        row_dict["req"][TRES_KEYS[key]] = value
    del row["tres_alloc"], row["tres_req"]
    row_dict["state"] = STATES[row.pop("state")]
    row_dict["job_db_inx"] = row.pop("job_db_inx")
    row_dict["timelimit"] = row.pop("timelimit")
    row_dict["id_job"] = row.pop("id_job")
    assert row == {}
    return row_dict

###

def get_all_jobs():
    creds = dotenv_values("../.env")
    conn = mariadb.connect(unix_socket="/var/lib/mysql/mysql.sock", user=creds["DB_USERNAME"], password=creds["DB_PASSWORD"], database=creds["PROCESSED_DATABASE"])
    with conn, conn.cursor(dictionary=True, buffered=False) as cursor:
        query = f"SELECT * FROM {creds['PROCESSED_TABLE']}"
        cursor.execute(query)
        for row in cursor:
            yield job_row_to_dict(row)

def get_all_jobs_from_parquet(filename):
    dataset = ds.dataset(filename, format="parquet")
    scanner = dataset.scanner()
    for record_batch in scanner.to_batches():
        yield from record_batch.to_pylist()

def get_jobs_from_db(start_window, end_window, jobs_file=None, by_submit_time=True):
    assert by_submit_time, "Shouldn't be using start and end time"
    creds = dotenv_values("../.env")
    conn = mariadb.connect(unix_socket="/var/lib/mysql/mysql.sock", user=creds["DB_USERNAME"], password=creds["DB_PASSWORD"], database=creds["PROCESSED_DATABASE"])
    with conn, conn.cursor(dictionary=True, buffered=False) as cursor:
        if by_submit_time:
            where_clause = "job_table.time_submit >= %s AND job_table.time_submit < %s"
        else:
            where_clause = "job_table.time_end >= %s AND job_table.time_end <= %s AND job_table.time_start >= %s"
        
        query = f"""
            SELECT
                *
            FROM 
                {creds["PROCESSED_TABLE"]} job_table
            WHERE
                {where_clause}
            ORDER BY
                job_table.job_db_inx ASC
        """

        if by_submit_time:
            cursor.execute(query, (start_window, end_window))
        else:
            cursor.execute(query, (start_window, end_window, start_window))

        for row in cursor:
            yield job_row_to_dict(row)

def get_jobs_from_parquet(start_window, end_window, filename, by_submit_time=True):
    assert by_submit_time, "Shouldn't be using start and end time"
    start_window, end_window = int(start_window), int(end_window)
    dataset = ds.dataset(filename, format="parquet")
    time_filter = (pc.field(("time", "submit")) >= start_window) & (pc.field(("time", "submit")) < end_window)
    scanner = dataset.scanner(filter=time_filter)
    for record_batch in scanner.to_batches():
        yield from record_batch.to_pylist()

###

BATCH_SECONDS = 60
CHUNK_SECONDS = 86400


def _iter_job_batches(start_window, end_window, batch_seconds, fetch_chunk_rows):
    start_window, end_window = int(start_window), int(end_window)
    for chunk_start in range(start_window, end_window, CHUNK_SECONDS):
        chunk_end = min(chunk_start + CHUNK_SECONDS, end_window)
        buckets = defaultdict(list)
        for row in fetch_chunk_rows(chunk_start, chunk_end):
            buckets[(row["time"]["submit"] - chunk_start) // batch_seconds].append(row)
        num_batches = math.ceil((chunk_end - chunk_start) / batch_seconds)
        for index in range(num_batches):
            yield buckets.pop(index, [])


def get_job_batches_from_parquet(start_window, end_window, filename, by_submit_time=True, batch_seconds=BATCH_SECONDS):
    assert by_submit_time, "Shouldn't be using start and end time"
    dataset = ds.dataset(filename, format="parquet")

    def fetch_chunk_rows(chunk_start, chunk_end):
        time_filter = (pc.field(("time", "submit")) >= chunk_start) & (pc.field(("time", "submit")) < chunk_end)
        return dataset.scanner(filter=time_filter).to_table().to_pylist()

    yield from _iter_job_batches(start_window, end_window, batch_seconds, fetch_chunk_rows)


def get_job_batches_from_db(start_window, end_window, jobs_file=None, by_submit_time=True, batch_seconds=BATCH_SECONDS):
    assert by_submit_time, "Shouldn't be using start and end time"

    def fetch_chunk_rows(chunk_start, chunk_end):
        return get_jobs_from_db(chunk_start, chunk_end, jobs_file, by_submit_time)

    yield from _iter_job_batches(start_window, end_window, batch_seconds, fetch_chunk_rows)
