import math
from collections import defaultdict

import pyarrow.compute as pc
import pyarrow.dataset as ds
import pymysql
import pymysql.cursors
from dotenv import dotenv_values

DEFAULT_DB_SOCKET = "/var/lib/mysql/mysql.sock"
DEFAULT_DB_PORT = 3306


def db_connect_kwargs(creds, database_key):
    # DB_HOST (+ optional DB_PORT) means TCP; otherwise DB_SOCKET, else the default socket
    kwargs = {"user": creds["DB_USERNAME"], "password": creds["DB_PASSWORD"], "database": creds[database_key]}
    if creds.get("DB_HOST"):
        return {**kwargs, "host": creds["DB_HOST"], "port": int(creds.get("DB_PORT") or DEFAULT_DB_PORT)}
    return {**kwargs, "unix_socket": creds.get("DB_SOCKET") or DEFAULT_DB_SOCKET}


def connect_db(creds, database_key):
    return pymysql.connect(**db_connect_kwargs(creds, database_key))


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

def _slurm_jobs_query(creds, where_clause):
    return f"""
            SELECT
                job_table.job_db_inx AS job_db_inx,
                job_table.id_job AS id_job,
                job_table.state AS state,
                job_table.time_start AS time_start,
                job_table.time_end AS time_end,
                job_table.time_submit AS time_submit,
                job_table.tres_alloc AS tres_alloc,
                job_table.tres_req AS tres_req,
                job_table.timelimit AS timelimit,
                CAST(SUM((step_table.user_sec * 1000000) + step_table.user_usec + (step_table.sys_sec * 1000000) + step_table.sys_usec) AS DECIMAL(65, 6)) / 1000000 AS total_cpu_time
            FROM
                {creds["JOB_TABLE"]} job_table
            JOIN
                {creds["STEP_TABLE"]} step_table ON job_table.job_db_inx = step_table.job_db_inx
            WHERE
                {where_clause}
            GROUP BY
                job_table.job_db_inx
            ORDER BY
                job_table.job_db_inx ASC
        """

def get_jobs_from_slurm_db(start_window, end_window):
    # raw Slurm accounting tables: only time_submit is indexed, so select by a submit window
    creds = dotenv_values("../.env")
    conn = connect_db(creds, "MAIN_DATABASE")
    with conn, conn.cursor(pymysql.cursors.SSDictCursor) as cursor:
        query = _slurm_jobs_query(creds, "job_table.time_submit >= %s AND job_table.time_submit < %s AND job_table.state = 3")
        cursor.execute(query, (int(start_window), int(end_window)))
        for row in cursor:
            yield job_row_to_dict(row)

def get_all_jobs():
    # every COMPLETED job in one query over the raw Slurm tables; state has no index, so this scans the whole job table
    creds = dotenv_values("../.env")
    conn = connect_db(creds, "MAIN_DATABASE")
    with conn, conn.cursor(pymysql.cursors.SSDictCursor) as cursor:
        cursor.execute(_slurm_jobs_query(creds, "job_table.state = 3"))
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
            yield chunk_start + index * batch_seconds, buckets.pop(index, [])


def get_job_batches_from_parquet(start_window, end_window, filename, by_submit_time=True, batch_seconds=BATCH_SECONDS):
    assert by_submit_time, "Shouldn't be using start and end time"
    dataset = ds.dataset(filename, format="parquet")

    def fetch_chunk_rows(chunk_start, chunk_end):
        time_filter = (pc.field(("time", "submit")) >= chunk_start) & (pc.field(("time", "submit")) < chunk_end)
        return dataset.scanner(filter=time_filter).to_table().to_pylist()

    yield from _iter_job_batches(start_window, end_window, batch_seconds, fetch_chunk_rows)

