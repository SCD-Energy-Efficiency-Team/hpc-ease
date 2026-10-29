import io
import json
import os
import random
import shlex
import shutil
import subprocess
import sys
import time
import tomllib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
from tqdm import tqdm

project_root = Path(__file__).resolve().parent.parent

app = typer.Typer()

DEFAULT_CLUSTERS_FILE = Path(__file__).resolve().parent / "clusters.toml"

# sacct truncates some names (OUT_OF_ME, CANCELLED+)
TERMINAL_STATES = {
    "COMPLETED", "FAILED", "TIMEOUT", "CANCELLED", "NODE_FAIL", "OUT_OF_MEMORY", "OUT_OF_ME",
    "BOOT_FAIL", "DEADLINE", "PREEMPTED", "REVOKED",
}

def is_terminal(state):
    return state_name(state) in TERMINAL_STATES

SLURM_CALL_TIMEOUT = 60
SLOW_CALL_SECONDS = 10
WAIT_NOTE_FIRST = 180
WAIT_NOTE_REPEAT = 600

def run_slurm(cmd):
    """Run a Slurm command and return (result, error); result is None if it timed out."""
    start = time.time()
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=SLURM_CALL_TIMEOUT)
    except subprocess.TimeoutExpired:
        return None, f"{cmd[0]} timed out after {SLURM_CALL_TIMEOUT}s"
    took = time.time() - start
    if took > SLOW_CALL_SECONDS:
        tqdm.write(f"{cmd[0]} took {took:.0f}s")
    return result, None

# before this the dataset is mostly test traffic, not user jobs
DEFAULT_FROM_DATE = "2025-02-01"

DEFAULT_WARMUP_DAYS = 10.0
# process.py's --retry-timeout default
PROCESS_DEFAULT_RETRY_TIMEOUT_HOURS = 72.0

DATASET_END = datetime.fromtimestamp(1780351201, tz=UTC)

def parse_walltime(text):
    """Slurm [D-]HH:MM:SS to seconds."""
    days, _, clock = text.rpartition("-")
    hours, minutes, seconds = (int(part) for part in clock.split(":"))
    return (int(days) if days else 0) * 86400 + hours * 3600 + minutes * 60 + seconds

def load_route(path, name=None):
    """Read one Slurm route from the clusters TOML; `name` or the file's top-level `default` picks it."""
    if not path.is_file():
        raise typer.BadParameter(f"{path} not found; copy benchmark/clusters.example.toml to it and edit", param_hint="--clusters-file")
    with open(path, "rb") as file:
        config = tomllib.load(file)
    routes = config.get("routes", {})
    name = name or config.get("default")
    if name is None:
        raise typer.BadParameter(f"--use-slurm needs --route (or a top-level `default` in {path}); routes there: {', '.join(routes) or 'none'}", param_hint="--route")
    if name not in routes:
        raise typer.BadParameter(f"route {name!r} is not in {path}; routes there: {', '.join(routes) or 'none'}", param_hint="--route")
    route = {"name": name, "exclude": [], "extra_args": [], **routes[name]}
    for key in ("account", "partition", "resources"):
        if key not in route:
            raise typer.BadParameter(f"route {name!r} in {path} has no `{key}`", param_hint="--clusters-file")
    max_time = parse_walltime(route["max_time"]) if "max_time" in route else None
    for row in route["resources"]:
        if "mem" not in row or "time" not in row:
            raise typer.BadParameter(f"route {name!r}: every resources row needs `mem` and `time`: {row}", param_hint="--clusters-file")
        if "qos" not in row and "qos" not in route:
            raise typer.BadParameter(f"route {name!r}: a row with no `qos` needs a route-level `qos`: {row}", param_hint="--clusters-file")
        if max_time is not None and parse_walltime(row["time"]) > max_time:
            raise typer.BadParameter(f"route {name!r}: row time {row['time']} is over max_time {route['max_time']}", param_hint="--clusters-file")
    return route

def route_resources(route, days):
    """(mem, time limit, qos) of the first resources row that fits `days` streamed days (a row with no max_days fits any)."""
    for row in route["resources"]:
        if row.get("max_days") is None or days <= row["max_days"]:
            return row["mem"], row["time"], row.get("qos", route.get("qos"))
    raise typer.BadParameter(f"{days:g} streamed days is past the last resources row of route {route['name']!r}; use a route that allows it or a shorter span", param_hint="--route")

TIME_PART = ["/usr/bin/time", "-f", '{"exit_code":%x,"max_ram_kib":%M,"user_time_sec":%U,"sys_time_sec":%S,"elapsed_sec":%e}']

def state_name(state):
    return state.split()[0].rstrip("+")

def build_process_cmd(day_string, days_to_fetch, project_folder, jobs_file, *, head_weight=None, head_fraction=None, max_wait=None, reschedule_percent=None, reschedule_seed=None, defer=True, retry_timeout=None, count_violations=False, warmup_days=0.0, cooldown_days=0.0, graph_mode="no-graph", dump_periods_dir=None):
    cmd = [
        *TIME_PART,
        str(project_folder / ".venv/bin/python3"),
        str(project_folder / "main/process.py"),
        "--print-json", "--graph-mode", graph_mode,
        "--chosen-day", day_string, "--days-to-fetch", str(days_to_fetch),
    ]
    if jobs_file is not None:
        cmd += ["--jobs-file", str(jobs_file.resolve())]
    optional = [("--head-weight", head_weight), ("--head-fraction", head_fraction), ("--max-wait", max_wait),
                ("--reschedule-percent", reschedule_percent), ("--reschedule-seed", reschedule_seed)]
    for flag, value in optional:
        if value is not None:
            cmd += [flag, str(value)]
    cmd.append("--defer" if defer else "--no-defer")
    if retry_timeout is not None:
        cmd += ["--retry-timeout", str(retry_timeout)]
    cmd.append("--count-violations" if count_violations else "--no-count-violations")
    cmd += ["--warmup-days", str(warmup_days), "--cooldown-days", str(cooldown_days)]
    if dump_periods_dir is not None:
        cmd += ["--dump-periods", str(Path(dump_periods_dir) / f"{day_string}_{days_to_fetch}d.json")]
    return cmd

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

    def submit(self, **kwargs):
        cmd = [self.TS_BINARY] + build_process_cmd(**kwargs)
        output = subprocess.run(cmd, capture_output=True, text=True, check=False).stdout
        return int(output.strip())

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
    def __init__(self, route, exclude_nodes=()):
        self.route = route
        self.exclude_nodes = list(exclude_nodes)
        self.last_error = None  # why the last check_queue returned None
        self.terminal = set()  # job ids sacct has reported finished
        self.pending_states = {}  # unfinished job id -> state from the last check_queue

    def build_cmd(self, day_string, days_to_fetch, project_folder, jobs_file, **flags):
        assert jobs_file is not None, "get_from_db should not work on Slurm"
        route = self.route
        mem, duration, qos = route_resources(route, days_to_fetch + flags["warmup_days"] + flags["cooldown_days"])
        slurm_part = [
            "sbatch", "-A", route["account"],
            "-p", route["partition"], "-q", qos, "-t", duration, f"--mem={mem}",
            "-N", "1", "-c", "1",
            "-o", "slurmoutputs/job_%j.log"
        ]
        if route.get("reservation"):
            slurm_part.append(f"--reservation={route['reservation']}")
        if self.exclude_nodes:
            slurm_part.append(f"--exclude={','.join(self.exclude_nodes)}")
        slurm_part.extend(route["extra_args"])
        wrap_part = shlex.join(build_process_cmd(day_string, days_to_fetch, project_folder, jobs_file, **flags))
        return slurm_part + [f"--wrap={wrap_part}"]

    def submit(self, **kwargs):
        cmd = self.build_cmd(**kwargs)
        try:
            process = subprocess.run(cmd, capture_output=True, text=True, check=True)
            output = process.stdout.strip()
            return int(output.split(" ")[-1])
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to queue job: {e.stderr}")

    def get_outputs(self, job_ids, filename):
        rows = []
        for job_id in job_ids:
            output = subprocess.run(["sacct", "-j", str(job_id), "-X", "--format=JobID,State,ExitCode,Elapsed"], capture_output=True, text=True, check=False).stdout
            line = output.strip().split("\n")[-1].strip()
            job_id_from_slurm, state, elevel, time = " ".join(line.split()).split(" ")
            assert job_id_from_slurm == str(job_id)
            if not is_terminal(state):
                raise RuntimeError(f"Job {job_id} is {state}, not finished; nothing written or deleted")
            rowdict = {"ID": job_id, "State": state, "E-Level": elevel, "Time": time}
            log_path = f"slurmoutputs/job_{job_id}.log"
            if os.path.isfile(log_path):
                with open(log_path) as file:
                    rowdict["Output"] = file.read()
            elif state_name(state) in ("CANCELLED", "NODE_FAIL"):
                rowdict["Output"] = ""  # cancelled or node died before it wrote a log
            else:
                raise FileNotFoundError(f"Job {job_id} is {state} but {log_path} is missing")
            rows.append(rowdict)
        with open(filename, "a") as logfile:
            for rowdict in rows:
                logfile.write(json.dumps(rowdict)+"\n")
        for job_id in job_ids:
            if os.path.isfile(f"slurmoutputs/job_{job_id}.log"):
                os.remove(f"slurmoutputs/job_{job_id}.log")

    def check_queue(self, job_ids):
        """Number of jobs not yet finished, or None if Slurm couldn't say."""
        squeue, error = run_slurm(["squeue", "--me", "-h", "-o", "%i %T"])
        if squeue is None or squeue.returncode != 0:
            self.last_error = error or f"squeue exited {squeue.returncode}: {squeue.stderr.strip()[:200]}"
            return None
        queued = {}
        for line in squeue.stdout.splitlines():
            job_id, _, state = line.partition(" ")
            if job_id.isdigit():
                queued[int(job_id)] = state.strip()
        self.terminal.difference_update(queued)  # a requeued job is running again
        # a job missing from squeue may not be finished, so confirm with sacct
        to_check = [job_id for job_id in job_ids if job_id not in queued and job_id not in self.terminal]
        unconfirmed = {}
        if to_check:
            sacct, error = run_slurm(["sacct", "-X", "-n", "-P", "--format=JobID,State", "-j", ",".join(map(str, to_check))])
            if sacct is None or sacct.returncode != 0:
                self.last_error = error or f"sacct exited {sacct.returncode}: {sacct.stderr.strip()[:200]}"
                return None
            for line in sacct.stdout.splitlines():
                job_id, _, state = line.partition("|")
                if job_id.isdigit():
                    if is_terminal(state):
                        self.terminal.add(int(job_id))
                    else:
                        unconfirmed[int(job_id)] = state
        self.pending_states = {job_id: queued.get(job_id) or unconfirmed.get(job_id, "no sacct row yet") for job_id in job_ids if job_id not in self.terminal}
        return len(self.pending_states)

    def describe_pending(self, limit=3):
        shown = list(self.pending_states.items())[:limit]
        return ", ".join(f"{job_id} {state}" for job_id, state in shown) + (", ..." if len(self.pending_states) > limit else "")

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
    route: Annotated[
        str | None,
        typer.Option(help="Slurm route from --clusters-file (needed with --use-slurm unless the file sets a default)"),
    ] = None,
    clusters_file: Annotated[
        Path,
        typer.Option(dir_okay=False, resolve_path=True, help="TOML describing the Slurm routes; see clusters.example.toml"),
    ] = DEFAULT_CLUSTERS_FILE,
    exclude_nodes: Annotated[
        list[str] | None,
        typer.Option(help="Slurm nodes to exclude, replacing the route's `exclude` list"),
    ] = None,
    no_exclude: Annotated[
        bool,
        typer.Option("--no-exclude", help="Don't exclude any Slurm nodes, overriding --exclude-nodes and the route's `exclude`"),
    ] = False,
    dates_file: Annotated[
        Path | None,
        typer.Option(
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help="Run only the dates listed in this file (one YYYY-MM-DD per line); default is every day in the dataset",
        ),
    ] = None,
    days_to_fetch: Annotated[
        list[int] | None,
        typer.Option(help="Span(s) in days; repeat the flag for several. Overrides --only-one-day."),
    ] = None,
    from_date: Annotated[
        str | None,
        typer.Option(
            help=f"Only submit days on or after this date (YYYY-MM-DD); default is {DEFAULT_FROM_DATE}, except that --dates-file is used as given"
        ),
    ] = None,
    no_from_date: Annotated[
        bool,
        typer.Option("--no-from-date", help="Submit every date, overriding --from-date and the default"),
    ] = False,
    head_weight: Annotated[
        float | None,
        typer.Option(help="Weight on the first part of each window when scoring (1.0 = flat)"),
    ] = None,
    head_fraction: Annotated[
        float | None,
        typer.Option(help="Share of the window the head weight covers"),
    ] = None,
    max_wait: Annotated[
        float | None,
        typer.Option(help="Latest a job may start after submit in hours"),
    ] = None,
    reschedule_percent: Annotated[
        float | None,
        typer.Option(help="Percent of jobs the scheduler moves; the rest keep their original times"),
    ] = None,
    reschedule_seed: Annotated[
        int | None,
        typer.Option(help="Seed for the subset --reschedule-percent picks"),
    ] = None,
    defer: Annotated[
        bool,
        typer.Option(help="Queue jobs with no free window and retry them (--no-defer pins them at once)"),
    ] = True,
    retry_timeout: Annotated[
        float | None,
        typer.Option(help="Hours a deferred job may stay unplaced"),
    ] = None,
    count_violations: Annotated[
        bool,
        typer.Option(help="Count pinned jobs that exceed node capacity (slower)"),
    ] = False,
    warmup_days: Annotated[
        float,
        typer.Option(help="Days before the window run through the scheduler unmeasured; 0 for an empty start"),
    ] = DEFAULT_WARMUP_DAYS,
    cooldown_days: Annotated[
        float | None,
        typer.Option(help="Days after the window run unmeasured; default is the retry timeout in days with --defer"),
    ] = None,
    graph_mode: Annotated[
        str,
        typer.Option(help="no-graph or save-graph (save-graph overwrites per date and span)"),
    ] = "no-graph",
    dump_periods_dir: Annotated[
        Path | None,
        typer.Option(file_okay=False, resolve_path=True, help="Enables --dump-periods in process.py, writes one <date>_<span>d.json per run here (must be visible to the Slurm nodes)"),
    ] = None,
    keep_truncated: Annotated[
        bool,
        typer.Option(
            "--keep-truncated",
            help="Also submit windows that end after the dataset does, by default they are skipped",
        ),
    ] = False,
):
    if not use_slurm:
        jobscheduler = TaskSpooler()
    else:
        assert jobs_file is not None, "get_from_db should not work on Slurm"
        slurm_route = load_route(clusters_file, route)
        if no_exclude:
            exclude_nodes = []
        elif exclude_nodes is None:
            exclude_nodes = slurm_route["exclude"]
        jobscheduler = Slurm(slurm_route, exclude_nodes)
        print(f"Slurm route {slurm_route['name']} from {clusters_file}")

    assert graph_mode in ("no-graph", "save-graph"), "--graph-mode must be no-graph or save-graph (show-graph needs a display)"
    combos = []

    if days_to_fetch:
        days_to_fetch_list = days_to_fetch
    elif only_one_day:
        days_to_fetch_list = [1]
    else:
        days_to_fetch_list = [1, 2, 4, 7]

    if dates_file is not None:
        dates = [line.strip() for line in dates_file.read_text().splitlines() if line.strip()]
    else:
        days = get_days()
        dates = [day.date().isoformat() for day in days]
    if cooldown_days is None:
        timeout_hours = retry_timeout if retry_timeout is not None else PROCESS_DEFAULT_RETRY_TIMEOUT_HOURS
        cooldown_days = timeout_hours / 24 if defer else 0.0
    print(f"warm-up {warmup_days:g} days, cool-down {cooldown_days:g} days")

    if from_date is None and dates_file is None:
        from_date = DEFAULT_FROM_DATE
    if from_date is not None and not no_from_date:
        cutoff = pd.Timestamp(from_date)
        kept = [date for date in dates if pd.Timestamp(date) >= cutoff]
        print(f"--from-date {from_date}: kept {len(kept)} of {len(dates)} dates")
        dates = kept
    if debug:
        print(f"As debugging, ignored {len(dates)}, using 2025-2-15 instead")
        dates = ["2025-2-15"]
    random.shuffle(dates)

    if use_slurm:
        for span in days_to_fetch_list:
            mem, duration, qos = route_resources(slurm_route, span + warmup_days + cooldown_days)  # fail before the snapshot
            print(f"{span}d: {mem}, {duration}, qos {qos}")
            ends = slurm_route.get("ends")
            if ends and datetime.now() + timedelta(seconds=parse_walltime(duration)) > datetime.fromisoformat(ends):
                print(f"WARNING: a {duration} job submitted now would run past the route's `ends` ({ends}), so Slurm will leave it pending")

    snapshot_folder, commit_hash = make_snapshot(Path.home() / "cprojsnaps", commit=commit)

    for span in days_to_fetch_list:
        span_dates = dates
        if not keep_truncated:
            end_cutoff = DATASET_END.replace(tzinfo=None) - timedelta(days=span + cooldown_days)
            span_dates = [date for date in dates if pd.Timestamp(date) <= end_cutoff]
            print(f"{span}d: skipped {len(dates) - len(span_dates)} of {len(dates)} dates whose window plus cool-down ends after the dataset")
        for date in span_dates:
            combos.append({
                "day_string": date, "days_to_fetch": span, "project_folder": snapshot_folder, "jobs_file": jobs_file,
                "head_weight": head_weight, "head_fraction": head_fraction, "max_wait": max_wait,
                "reschedule_percent": reschedule_percent, "reschedule_seed": reschedule_seed, "defer": defer,
                "retry_timeout": retry_timeout, "count_violations": count_violations, "warmup_days": warmup_days,
                "cooldown_days": cooldown_days, "graph_mode": graph_mode, "dump_periods_dir": dump_periods_dir,
            })

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
        job_ids.append(jobscheduler.submit(**combo))

    total_jobs = len(combos)
    print(f"Submitted {total_jobs} jobs, waiting for them to finish...")

    failed_checks = 0
    last_count, last_change, last_note = None, time.time(), 0.0
    with tqdm(total=total_jobs) as pbar:
        while True:
            num_jobs = jobscheduler.check_queue(job_ids)
            now = time.time()
            if num_jobs != last_count:
                last_count, last_change, last_note = num_jobs, now, 0.0
            elif num_jobs and hasattr(jobscheduler, "describe_pending") and now - last_change >= WAIT_NOTE_FIRST and now - last_note >= WAIT_NOTE_REPEAT:
                last_note = now
                tqdm.write(f"Still waiting on {num_jobs} job(s) after {(now - last_change) / 60:.0f} min: {jobscheduler.describe_pending()}")
            if num_jobs is None:
                failed_checks += 1
                if failed_checks == 1 or failed_checks % 60 == 0:
                    tqdm.write(f"Slurm didn't answer ({getattr(jobscheduler, 'last_error', 'unknown')}); retrying every 5s, failed checks so far: {failed_checks}")
            else:
                if failed_checks:
                    tqdm.write(f"Slurm is answering again after {failed_checks} failed check(s)")
                    failed_checks = 0
                pbar.n = total_jobs - num_jobs
                pbar.refresh()
                if num_jobs == 0:
                    break
            time.sleep(5)

    shutil.rmtree(snapshot_folder)

    print("Gathering results...")
    suffix = f"-{commit_hash}" if commit_hash is not None else ""
    if use_slurm:
        suffix += f"-{slurm_route['name']}"
    if head_weight is not None:
        suffix += f"-hw{head_weight:g}"
    if head_fraction is not None:
        suffix += f"-hf{head_fraction:g}"
    if max_wait is not None:
        suffix += f"-mw{max_wait:g}"
    if reschedule_percent is not None:
        suffix += f"-rp{reschedule_percent:g}"
    if reschedule_seed is not None:
        suffix += f"-rs{reschedule_seed}"
    suffix += "-defer" if defer else "-nodefer"
    if retry_timeout is not None:
        suffix += f"-rt{retry_timeout:g}"
    suffix += f"-wu{warmup_days:g}-cd{cooldown_days:g}"
    while True:
        random_filename = f"temp/{int(time.time()*1000)!s}{suffix}.txt"
        if not os.path.isfile(random_filename):
            break
    jobscheduler.get_outputs(job_ids, random_filename)

if __name__ == "__main__":
    app()
