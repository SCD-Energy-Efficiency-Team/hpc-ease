from datetime import datetime, timedelta
import os
import subprocess
import io
import pandas as pd
import json
import typer
import random
from enum import Enum

TS_BINARY = "../../task-spooler/ts"
CUSTOM_SOCKET = "/tmp/carboncoding.socket"

app = typer.Typer()

def get_days():
    start_range, end_range = 1725608400, 1780351201
    start_range, end_range = datetime.fromtimestamp(start_range), datetime.fromtimestamp(end_range)
    start_window = start_range.replace(hour=0, minute=0, second=0, microsecond=0)
    days = []
    while start_window < end_range:
        days.append(start_window)
        start_window += timedelta(days=1)
    return days

def run_ts(params, get_output=False):
    # TS_SOCKET=/tmp/carboncoding.socket ../../task-spooler/ts
    env = os.environ.copy()
    env["TS_SOCKET"] = CUSTOM_SOCKET
    cmd = [TS_BINARY] + params
    if not get_output:
        subprocess.run(cmd, env=env)
    else:
        output = subprocess.run(cmd, env=env, capture_output=True, text=True).stdout
        return output

def submit(day_string):
    env = os.environ.copy()
    env["TS_SOCKET"] = CUSTOM_SOCKET

    time_part = ["/usr/bin/time", "-f", '{"exit_code":%x,"max_ram_kb":%M,"user_time_sec":%U,"sys_time_sec":%S,"elapsed_sec":%e}']
    python_part = ["../.venv/bin/python3", "../main/process.py", "--print-json", "--chosen-day", day_string]
    cmd = [TS_BINARY] + time_part + python_part

    try:
        subprocess.run(cmd, env=env, stdout=subprocess.DEVNULL, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Failed to queue job: {e.stderr}")

def get_ts_outputs():
    output = run_ts(["-l"], get_output=True)
    df = pd.read_fwf(io.StringIO(output), infer_nrows=10000)
    finished_tasks = df[(df["State"] == "finished")]
    keys = ["ID", "State", "E-Level", "Time"]
    with open("tsdonejobs.txt", "a") as logfile:
        for index, row in finished_tasks.iterrows():
            rowdict = {key: row[key] for key in keys}
            with open(row["Output"]) as file:
                content = file.read()
            rowdict["Output"] = content
            logfile.write(json.dumps(rowdict)+"\n")
            run_ts(["-r", str(rowdict["ID"])])

class Mode(str, Enum):
    grab_only = "grab-only"
    grab_and_kill = "grab-and-kill"
    full_run = "full-run"

@app.command()
def run_main(
    mode: Mode = Mode.grab_only,
    test_dates: bool = False,
):
    """
    with open("tsdonejobs.txt") as file:
        for line in file:
            line = json.loads(line)
            if line["ID"] == 82:
                print(line["Output"])
    quit()
    """

    if test_dates and mode != Mode.full_run:
        raise typer.BadParameter(
            f"When --test-dates is enabled, --mode must be '{Mode.full_run.value}'."
        )

    get_ts_outputs()
    if mode == Mode.grab_only:
        quit()
    
    input("Press enter to kill previous ts server...")
    run_ts(["-C"]) # Clear finished jobs from list
    run_ts(["-T"]) # Stops all running jobs
    run_ts(["-K"]) # Kills the ts server
    run_ts(["-S", "14"]) # Sets the ts server to run x jobs concurrently
    if mode == Mode.grab_and_kill:
        quit()

    count = 0
    if test_dates:
        for date in ["2025-11-9", "2026-5-4", "2025-1-9", "2025-2-15"]:
            submit(date)
            count += 1
    else:
        days = get_days()
        random.shuffle(days)
        for day in days:
            submit(day.date().isoformat())
            count += 1
    print(f"Submitted {count} jobs")

if __name__ == "__main__":
    app()
