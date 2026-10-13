import io
import json
import os
import random
import shlex
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
from tqdm import tqdm

project_root = Path(__file__).resolve().parent.parent

app = typer.Typer()

# host1262 has no /usr/bin/time, so every job landed there dies with exit 127
DEFAULT_EXCLUDE_NODES = ["host1262"]

def get_days():
    start_range, end_range = 1725608400, 1780351201
    start_range, end_range = datetime.fromtimestamp(start_range, tz=UTC), datetime.fromtimestamp(end_range, tz=UTC)
    start_window = start_range.replace(hour=0, minute=0, second=0, microsecond=0)
    days = []
    while start_window < end_range:
        days.append(start_window)
        start_window += timedelta(days=1)
    return days

class TaskSpooler:
    def __init__(self):
        self.TS_BINARY = project_root.parent / "task-spooler/ts"

    def run_cmd(self, params, get_output=False):
        cmd = [self.TS_BINARY] + params
        if not get_output:
            subprocess.run(cmd, check=False)
        else:
            output = subprocess.run(cmd, capture_output=True, text=True, check=False).stdout
            return output

    def submit(self, day_string, days_to_fetch, project_folder, jobs_file=None):
        time_part = ["/usr/bin/time", "-f", '{"exit_code":%x,"max_ram_kib":%M,"user_time_sec":%U,"sys_time_sec":%S,"elapsed_sec":%e}']
        python_part = [
            str(project_folder / ".venv/bin/python3"),
            str(project_folder / "main/process.py"),
            "--print-json", "--graph-mode", "no-graph",
            "--chosen-day", day_string,
            "--days-to-fetch", str(days_to_fetch)
        ]
        if jobs_file is not None:
            python_part.extend(["--jobs-file", str(jobs_file.resolve())])
        cmd = [self.TS_BINARY] + time_part + python_part

        try:
            output = subprocess.run(cmd, capture_output=True, text=True, check=False).stdout
            return int(output.strip())
        except subprocess.CalledProcessError as e:
            print(f"Failed to queue job: {e.stderr}")
        
        time.sleep(0.01)

    def get_outputs(self, job_ids, filename):
        output = self.run_cmd(["-l"], get_output=True)
        df = pd.read_fwf(io.StringIO(output), infer_nrows=10000)
        finished_tasks = df[(df["State"] == "finished")]
        keys = ["ID", "State", "E-Level", "Time"]
        with open(filename, "a") as logfile:
            for index, row in finished_tasks.iterrows():
                rowdict = {key: row[key] for key in keys}
                if rowdict["ID"] in job_ids:
                    if row["Output"] == "(...)":
                        continue
                    with open(row["Output"]) as file:
                        content = file.read()
                    rowdict["Output"] = content
                    logfile.write(json.dumps(rowdict)+"\n")
                    self.run_cmd(["-r", str(rowdict["ID"])])

    def check_queue(self, job_ids):
        lines = self.run_cmd(["-l"], get_output=True).splitlines()[1:]
        active_count = 0
        for line in lines:
            job_id, state = " ".join(line.split()).split(" ")[0:2]
            job_id = int(job_id)
            if job_id in job_ids and state in ["running", "queued"]:
                active_count += 1
        return active_count

class Slurm:
    def __init__(self, exclude_nodes=()):
        self.exclude_nodes = list(exclude_nodes)

    def submit(self, day_string, days_to_fetch, project_folder, jobs_file=None):
        assert jobs_file is not None, "get_from_db should not work on Slurm"
        partition = "standard" # "standard" "debug"
        qos = "standard" # "standard" "debug"
        duration = "12:00:00" # "01:00:00" "24:00:00"
        mem = "4G" # "1G" "4G"
        slurm_part = [
            "sbatch", "-A", "no-project",
            "-p", partition, "-q", qos, "-t", duration, f"--mem={mem}",
            "-N", "1", "-c", "1",
            "-o", "slurmoutputs/job_%j.log"
        ]
        if self.exclude_nodes:
            slurm_part.append(f"--exclude={','.join(self.exclude_nodes)}")
        time_part = ["/usr/bin/time", "-f", '{"exit_code":%x,"max_ram_kib":%M,"user_time_sec":%U,"sys_time_sec":%S,"elapsed_sec":%e}']
        python_part = [
            str(project_folder / ".venv/bin/python3"),
            str(project_folder / "main/process.py"),
            "--print-json", "--graph-mode", "no-graph",
            "--chosen-day", day_string, "--days-to-fetch", str(days_to_fetch), "--jobs-file", str(jobs_file.resolve())
        ]
        wrap_part = shlex.join(time_part + python_part)
        cmd = slurm_part + [f"--wrap={wrap_part}"]

        try:
            process = subprocess.run(cmd, capture_output=True, text=True, check=True)
            output = process.stdout.strip()
            return int(output.split(" ")[-1])
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to queue job: {e.stderr}")

    def get_outputs(self, job_ids, filename):
        with open(filename, "a") as logfile:
            for job_id in job_ids:
                output = subprocess.run(["sacct", "-j", str(job_id), "-X", "--format=JobID,State,ExitCode,Elapsed"], capture_output=True, text=True, check=False).stdout
                line = output.strip().split("\n")[-1].strip()
                job_id_from_slurm, state, elevel, time = " ".join(line.split()).split(" ")
                assert job_id_from_slurm == str(job_id)
                rowdict = {"ID": job_id, "State": state, "E-Level": elevel, "Time": time}
                with open(f"slurmoutputs/job_{job_id}.log") as file:
                    content = file.read()
                rowdict["Output"] = content
                logfile.write(json.dumps(rowdict)+"\n")
        for job_id in job_ids:
            os.remove(f"slurmoutputs/job_{job_id}.log")

    def check_queue(self, job_ids):   
        lines = subprocess.run(["squeue", "--me"], capture_output=True, text=True, check=False).stdout.splitlines()[1:]
        active_count = 0
        for line in lines:
            job_id = " ".join(line.split()).split(" ")[0]
            job_id = int(job_id)
            if job_id in job_ids:
                active_count += 1
        return active_count

def make_snapshot(snapshots_root, commit=None):
    snapshots_root.mkdir(exist_ok=True)
    start = time.time()
    while True:
        snap = snapshots_root / f"snap-{datetime.now(UTC):%Y%m%dT%H%M%S}-{random.randint(0, 0xFFFFFFFF):08x}"
        if not snap.is_dir():
            break
        if (time.time() - start) > 10:
            raise Exception("Creating a snapshot folder took too long?")

    commit_hash = None
    if commit is None:
        patterns = [
            "__pycache__", ".git", ".venv",
            "*.pyc", "*.parquet", "*.png",
            "carbondata.db", "uv.lock"
        ]

        print("Creating snapshot folder from working tree...")
        shutil.copytree(
            project_root,
            snap,
            ignore=shutil.ignore_patterns(*patterns),
        )
    else:
        resolved = subprocess.run(
            ["git", "rev-parse", "--verify", f"{commit}^{{commit}}"],
            cwd=project_root, check=True, capture_output=True, text=True,
        )
        commit_hash = resolved.stdout.strip()[:8]

        print(f"Creating snapshot folder from commit {commit}...")
        snap.mkdir()
        archive = subprocess.run(
            ["git", "archive", commit],
            cwd=project_root, check=True, capture_output=True,
        )
        subprocess.run(["tar", "-x"], cwd=snap, input=archive.stdout, check=True)

        # regional_carbon_intensity.csv is gitignored, so git archive skips it
        carbon_csv = project_root / "main/regional_carbon_intensity.csv"
        if carbon_csv.is_file():
            shutil.copy(carbon_csv, snap / "main/regional_carbon_intensity.csv")

    try:
        subprocess.run(
            ["uv", "sync"],
            cwd=snap,
            check=True,
            capture_output=True,
            text=True
        )
    except subprocess.CalledProcessError as e:
        print(f"\nERROR: 'uv sync' failed with exit code {e.returncode}\n{e.stderr.strip()}\n", file=sys.stderr)
        raise Exception("uv sync failed")

    print(f"Snapshot folder {snap} created.")
    return snap, commit_hash

@app.command()
def run_main(
    test_dates: bool = True,
    only_one_day: bool = True,
    jobs_file: Annotated[
        Path | None, 
        typer.Option(
            exists=True, 
            file_okay=True, 
            dir_okay=False, 
            readable=True, 
            resolve_path=True
        )
    ] = None,
    use_slurm: bool = False,
    debug: bool = False,
    commit: Annotated[
        str | None,
        typer.Option(help="Benchmark this git commit/branch/tag instead of the working tree"),
    ] = None,
    exclude_nodes: Annotated[
        list[str] | None,
        typer.Option(help=f"Slurm nodes to exclude (default: {', '.join(DEFAULT_EXCLUDE_NODES)})"),
    ] = None,
    no_exclude: Annotated[
        bool,
        typer.Option("--no-exclude", help="Don't exclude any Slurm nodes, overriding --exclude-nodes and the default"),
    ] = False,
):
    if no_exclude:
        exclude_nodes = []
    elif exclude_nodes is None:
        exclude_nodes = DEFAULT_EXCLUDE_NODES
    if not use_slurm:
        jobscheduler = TaskSpooler()
    else:
        assert jobs_file is not None, "get_from_db should not work on Slurm"
        jobscheduler = Slurm(exclude_nodes)

    combos = []

    if only_one_day:
        days_to_fetch_list = [1]
    else:
        days_to_fetch_list = [1, 2, 4, 7]

    if test_dates:
        with open("dates.txt") as file:
            dates = file.read().strip().split("\n")
    else:
        days = get_days()
        dates = [day.date().isoformat() for day in days]
    if debug:
        print(f"As debugging, ignored {len(dates)}, using 2025-2-15 instead")
        dates = ["2025-2-15"]
    random.shuffle(dates)

    snapshot_folder, commit_hash = make_snapshot(Path("/home/users/jamesrav/cprojsnaps"), commit=commit)

    # db_folder = Path(project_root) / "dbexports"
    # db_folder = Path("/work/scratch-pw5/jamesrav") / "dbexports"
    for days_to_fetch in days_to_fetch_list:
        for date in dates:
            combos.append((date, days_to_fetch, snapshot_folder, jobs_file))

    if debug:
        print(f"As debugging, reduced {len(combos)} down to 1")
        combos = [combos[0]]
    random.shuffle(combos)

    if not use_slurm:
        num_workers = jobscheduler.run_cmd(["-S"], get_output=True).strip()
        print(f"Number of workers is {num_workers}")
        print(f"Change with {jobscheduler.TS_BINARY} -S insert_num_workers_here")

    if sys.stdin.isatty():
        input(f"Will submit {len(combos)} jobs. Press enter to continue...")

    job_ids = []
    for combo in tqdm(combos, mininterval=0):
        job_ids.append(jobscheduler.submit(*combo))

    total_jobs = len(combos)
    print(f"Submitted {total_jobs} jobs, waiting for them to finish...")

    with tqdm(total=total_jobs) as pbar:
        while True:
            num_jobs = jobscheduler.check_queue(job_ids)
            pbar.n = total_jobs - num_jobs
            pbar.refresh()
            if num_jobs == 0:
                break
            time.sleep(5)
    
    shutil.rmtree(snapshot_folder)

    print("Gathering results...")
    suffix = f"-{commit_hash}" if commit_hash is not None else ""
    while True:
        random_filename = f"temp/{int(time.time()*1000)!s}{suffix}.txt"
        if not os.path.isfile(random_filename):
            break
    jobscheduler.get_outputs(job_ids, random_filename)

if __name__ == "__main__":
    app()
