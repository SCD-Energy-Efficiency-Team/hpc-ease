# Usage

## Table of contents

- [1. Describe your dataset via `dataset.toml`](#1-describe-your-dataset-via-datasettoml)
- [2. Export jobs to Parquet: `toparquet.py`](#2-export-jobs-to-parquet-toparquetpy)
- [3. Simulate one window: `process.py`](#3-simulate-one-window-processpy)
  - [All flags (for `process.py`)](#all-flags-for-processpy)
    - [Main flags (`process.py`)](#main-flags-processpy)
    - [Tuning flags for the algorithm](#tuning-flags-for-the-algorithm)
    - [Limiting what moves](#limiting-what-moves)
  - [Reading the output](#reading-the-output)
- [4. Sweep many windows](#4-sweep-many-windows)
  - [`runmultiple.py`](#runmultiplepy)
    - [Main flags (`runmultiple.py`)](#main-flags-runmultiplepy)
    - [Flags passed straight through to `process.py`](#flags-passed-straight-through-to-processpy-see-its-section-for-more-information)
  - [`clusters.toml`: where the Slurm jobs run](#clusterstoml-where-the-slurm-jobs-run)
  - [`benchmark/process.py`: logs to CSV](#benchmarkprocesspy-logs-to-csv)
  - [`compare.py`: compare two sweeps](#comparepy-compare-two-sweeps)

## 1. Describe your dataset via `dataset.toml`

Copy `dataset.example.toml` to `dataset.toml` in the repo root and edit it. **The scripts stop with an error if `dataset.toml` is missing**, and there is no fallback to LOTUS values. The file is git-ignored.

| Section | Key | Meaning |
|---|---|---|
| `[cluster]` | `nodes` | Number of nodes in the simulated cluster |
| | `cores_per_node` | Number of cores a node offers to jobs (across all sockets) |
| | `mem_gib_per_node` | Memory per node in GiB |
| `[energy]` | `p_idle_w`, `p_max_w` | Idle and maximum node power in watts. The script scales energy usage by CPU utilisation linearly between these two values and also by the share of a node's cores a job holds |
| `[carbon]` | `csv` | Carbon-intensity CSV, path relative to the repo root |
| | `column` | Which column (region) of the CSV to use |
| | `forecast_horizon_hours` | How far ahead a scheduler could see the grid, and the longest therefore a job will move (96 for the UK API) |
| `[dataset]` | `start`, `end` | Span of the job data, epoch seconds or TOML datetimes (UTC); jobs outside it are never requested |
| | `from_date` | Earliest date analysed by default, used by `runmultiple.py`. Use it to skip test traffic or a ramp-up period. |

All nodes are assumed identical: mixed node types and partitions are not supported at the moment.

**Carbon CSV.** A `datetime` column (ISO, UTC) of **30-minute** slots and one gCO2/kWh column per region. The slot length is fixed at 30 minutes, and the loader refuses a CSV with a slot that is not on the hour or half hour; hourly data has to be repeated into two slots. A gap in the data raises `MissingCarbonData`. The bundled `main/regional_carbon_intensity.csv` is UK data; for a cluster elsewhere you need to supply your own.

`process.py` and `runmultiple.py` take `--dataset-config PATH` to use a different file; the other scripts read `dataset.toml`. This is not to be confused with `benchmark/clusters.toml`, which says where the benchmark's Slurm jobs run.

## 2. Export jobs to Parquet: `toparquet.py`

The simulation reads a Parquet file of jobs, passed with `--jobs-file`; `process.py` stops with an error without it. `toparquet.py` builds that file from the raw Slurm accounting tables.

**Database settings** go in `.env` in the repo root (git-ignored; copy `.env.example`). `toparquet.py` needs the connection settings and three names:

| Variable | Meaning |
|---|---|
| `DB_USERNAME`, `DB_PASSWORD` | Credentials; a read-only account is enough |
| `DB_HOST` (+ `DB_PORT`) | Connect over TCP |
| `DB_SOCKET` | Connect over a Unix socket. With none of the three set, `/var/lib/mysql/mysql.sock` is used |
| `MAIN_DATABASE` | Database holding Slurm's accounting tables |
| `JOB_TABLE` | The job table (the Slurm job table, with your site's prefix) |
| `STEP_TABLE` | The job-step table |

```bash
cd main
uv run toparquet.py                      # one query for every COMPLETED job -> ../dbexports/all_jobs_full.parquet
uv run toparquet.py --batch-length 24    # multiple queries (24-hour windows) -> ../dbexports/all_jobs_perwindow24.parquet
uv run toparquet.py --mode count         # print row counts of existing exports
uv run toparquet.py --mode compare       # hash-compare the exports in ../dbexports against each other (they should match)
```

| Flag | Meaning |
|---|---|
| `--mode dump` (default) | Query the database and write a Parquet file in `../dbexports`. **An existing file with the same name is deleted first.** |
| `--mode count` | Print the row count of each `../dbexports/*.parquet` |
| `--mode compare` | Group the exports in `../dbexports` by content hash, to check two dumps agree |
| `--batch-length HOURS` | (detailed below) |

The windowed dump takes its date span from `dataset.toml`; the whole-table query does not read it and simply reads the entire database.

**What the dump reads.** The job table is joined to the step table, only jobs with `state = 3` (COMPLETED) are kept, and the steps' CPU time is summed into `total_cpu_time`. Consequences:

- **Only COMPLETED jobs are exported.** Jobs in other states (failed, cancelled, timed out) are not in the Parquet, so the simulated workload is the completed jobs only.
- **Whole table or windows.** Without the **--batch-length** parameter, the script performs one whole-table query to write a file named `all_jobs_full.parquet`. If you wish to be more cautious (e.g. to not cause an OOM if your database does not cap queries), you can iterate over a specified number of hours of submit time which would create a file called `all_jobs_perwindow<HOURS>.parquet` - this is identical to the existing `all_jobs_full.parquet` and either can be used for the scripts, but are named differently to not overwrite each other. `time_submit` should be indexed on the raw job table to perform a windowed query.
- From tests with the LOTUS cluster, a cold 30-day sample of the windowed query ran at about 85 to 92 microseconds per row, which extrapolates to roughly 1.5 to 1.6 hours for 64.2M rows, runtime will depend on how many jobs you have and the performance of your database.

The query assumes Slurm's standard column names (`time_submit`, `tres_alloc`, `tres_req`, `user_sec`, `user_usec`, `sys_sec`, `sys_usec`, and so on). **The raw table names are site-specific** (they carry a site prefix), which is why they come from `.env`. `STATES`, `TRES_KEYS` and the hardcoded `state = 3` in `main/fetchjobs.py` decode Slurm's numeric state and TRES ids - these may be specific to LOTUS, another cluster may need them changed. They are not used when reading a Parquet file.

If you can build the Parquet some other way, you do not need `toparquet.py` or a database. Each Parquet row is a job with these nested fields:

- `time`: `start`, `end`, `elapsed`, `submit` (epoch seconds) and `total` (total CPU time in seconds)
- `req`: `cpu`, `mem` (MiB) and `node`, as totals across all of the job's nodes
- `alloc`: `cpu` (total allocated CPUs)
- `timelimit` (minutes), `state`, `id_job`, `job_db_inx`

Although the exporter also writes `alloc.mem`, `alloc.node`, `alloc.energy`, `alloc.billing` and `req.billing`, the scripts do not currently use them. Jobs with a `start` or `end` of 0, which Slurm records for a job cancelled while pending, are skipped and counted rather than treated as an error (`never_ran_skipped` in the JSON line). Since the export keeps only COMPLETED jobs, these only appear if you build the Parquet another way.

## 3. Simulate one window: `process.py`

```bash
cd main
uv run process.py --chosen-day 2025-02-15 --days-to-fetch 1 \
    --jobs-file ../dbexports/all_jobs_full.parquet \
    --print-json --graph-mode no-graph --no-do-ask
```

This example command reads one day of jobs (selected by submit time), reschedules them into lower-carbon windows on the simulated cluster, and prints one JSON line with the energy and carbon before (`energy`, `carbon`) and after (`energy_2`, `carbon_2`), plus wait times and how many jobs could not be moved. Without `--print-json` it prints a summary and, unless `--no-do-ask` is given, waits for Enter after loading the jobs. `--jobs-file` is required: without it `process.py` stops with an error.

### All flags (for `process.py`)

Boolean flags come as `--x` / `--no-x` pairs; the default is shown.

#### Main flags (`process.py`)

| Flag | Default | Meaning |
|---|---|---|
| `--chosen-day YYYY-MM-DD` | Random day in the dataset | First day of the measured window (UTC). If default, it finds a random day with a window that contains jobs. |
| `--days-to-fetch N` | `1` | Length of the measured window in days |
| `--jobs-file PATH` | Required | Parquet file of jobs |
| `--dataset-config PATH` | `dataset.toml` in the repo root | Cluster, energy, carbon and dataset-span config |
| `--print-json` / `--no-print-json` | Off | Print one JSON result line instead of the text summary |
| `--graph-mode` | `save-graph` | Can be set to `no-graph`, `save-graph` (four-panel PNG in `../graphs/`) or `show-graph` (opens the matplotlib graph for interactive viewing) |
| `--do-ask` / `--no-do-ask` | On | Wait for the Enter key after loading jobs (skipped with `--print-json`) |
| `--compare-algorithm` / `--no-compare-algorithm` | On | Run the rescheduler; off gives only the original schedule's energy and carbon |
| `--dump-periods PATH` | None (off) | Save the window's original and rescheduled 5-minute series, the grid intensity and the scheduler's stats as JSON, which can be used to plot graphs |
| `--strict` / `--no-strict` | Off | Raise `VirtualCapacityExhausted` instead of pinning a job that is crowded out |
| `--count-violations` / `--no-count-violations` | Off | At pin time, check whether the pinned job's original window really had room on some node, and report the pins that did not (`violating_pin_*`). It changes no placement but is slower (about 1.6x total CPU in earlier measurements) and uses somewhat more memory. |

#### Tuning flags for the algorithm

| Flag | Default | Meaning |
|---|---|---|
| `--head-weight W` | `4.0` | A candidate window's first part counts W times the rest when scoring its carbon intensity. Users frequently overestimate how long their jobs will take so this attempts to account for this. `1.0` is flat scoring. |
| `--head-fraction F` | `0.25` | The share of the window (from the start) the head weight covers |
| `--defer` / `--no-defer` | On | A job that finds no free window is queued and retried as the forecast horizon advances, instead of being pinned at once |
| `--retry-timeout HOURS` | `72` | How long after submit a deferred job may stay unplaced before it is pinned |
| `--warmup-days N` | `10` | Days of jobs before the window run through the scheduler but not counted. `0` gives an empty start. |
| `--cooldown-days M` | Retry timeout in days with `--defer` (3 at 72h), otherwise `0` | Days of jobs after the window run but not counted |

- **Warm-up and cool-down.** Placement is simulated on an initially empty cluster, so a window would otherwise start with every low-carbon slot free and end with deferred jobs competing with nothing (on LOTUS this inflated savings by around 3 to 4 percent). `--warmup-days` and `--cooldown-days` mitigate this by loading the cluster with original unmoved jobs at the start and end for the specified number of days.
- **Jobs that cannot be placed.** A job is pinned at its original start (and counted in the output) when it declares more than the forecast horizon, is physically too big for the cluster, has too few candidate windows, or finds every window full.

#### Limiting what moves

| Flag | Default | Meaning |
|---|---|---|
| `--max-wait HOURS` | None (full forecast horizon) | Latest a job's window may start after its submit time, to within 30 minutes. Jobs pinned at their original start are not bound by it. |
| `--reschedule-percent P` | `100` | Move only P% of jobs; the rest keep their original times and still occupy capacity. Each job is picked independently from a hash of the seed and its id, so the random choosing of jobs is deterministic. |
| `--reschedule-seed N` | `0` | Seed for what jobs `--reschedule-percent` picks, so different seeds give independent and reproducible subsets |

### Reading the output

`--print-json` prints one line. The main fields are:

- `num_jobs`, `chosen_day`, `days_to_fetch`
- `energy`, `carbon` (original) and `energy_2`, `carbon_2` (rescheduled); the saving is `(carbon − carbon_2) / carbon`
- `wait_median`, `wait_p90`, `wait_p95`, `wait_p99`, `wait_max` for the original run, and the same with `_2` for the rescheduled run, in seconds as `start − submit`. `wait_*_2` includes jobs left at their original start, so `--max-wait` does not bound it. `wait_*_placed_2` covers only jobs the algorithm placed, so the cap bounds its maximum.
- Counts and energy of jobs left at their original start, split by reason: `unschedulable_*`, `crowded_out_*` (only with `--no-defer`), `never_placed_*` (deferred and timed out), `left_alone_jobs` (not chosen by `--reschedule-percent`); plus `deferred_jobs` and `deferred_placed`
- If `--count-violations` is enabled: also `violating_pin_jobs`, `violating_pin_energy`, `unverifiable_pin_*`
- `never_ran_skipped`, counts jobs with start == 0 or end == 0, only present if non-zero

`wait_negative`, `wait_negative_2` and `wait_negative_placed_2` count jobs with a negative wait (start < submit) in the original run, the rescheduled run, and the placed jobs only. The two `_2` fields should be 0: anything else means the scheduler placed a job before its submit time, which is a bug.

## 4. Sweep many windows

`benchmark/runmultiple.py` runs `process.py` over many dates and spans, either with [Task Spooler](https://viric.name/soft/ts/) run locally or on Slurm (`--use-slurm`). `benchmark/process.py` turns the raw logs into a CSV and `benchmark/compare.py` compares two such CSVs. Run all three from `benchmark/`.

### `runmultiple.py`

It snapshots the repo (or `--commit`) to `~/cprojsnaps/snap-*` and runs `uv sync` there, so the code under test is frozen while you keep editing. It also copies `dataset.toml` and the carbon CSV into the snapshot, so a sweep is frozen to the config it started with. It then submits one `process.py` run per date and span, waits for them all to finish, and writes one JSON line per run to `benchmark/temp/<timestamp>-<suffix>.txt` for each. The suffix records the commit, route and the flags that differ from the defaults (`-hw`, `-hf`, `-mw`, `-rp`, `-rs`, `-defer`/`-nodefer`, `-rt`, `-wu`, `-cd`), which is how you tell logs apart later.

```bash
cd benchmark
# local Task Spooler
uv run runmultiple.py --days-to-fetch 7 --jobs-file ../dbexports/all_jobs_full.parquet
# using Slurm
uv run runmultiple.py --use-slurm --route myroute --days-to-fetch 7 --jobs-file /path/on/the/cluster/all_jobs_full.parquet
```

Task Spooler runs use `--ts-binary PATH` if given, otherwise `tsp` from `PATH`, and stop with an error if there is neither. A binary named `ts` is never picked up from `PATH` (moreutils uses that name), so pass it with `--ts-binary`. On Slurm, `--jobs-file` must be a path the compute nodes can read.

#### Main flags (`runmultiple.py`)

| Flag | Default | Meaning |
|---|---|---|
| `--jobs-file PATH` | Required | Parquet file of jobs passed to every run |
| `--days-to-fetch N` | `1` | Window length in days; repeat the flag for several spans (`--days-to-fetch 1 --days-to-fetch 2 ...`) |
| `--dates-file PATH` | None | Run only the dates listed in the file, one `YYYY-MM-DD` per line; otherwise every day in the dataset |
| `--from-date YYYY-MM-DD` | `from_date` in `dataset.toml` | Skip dates before this, judged on each window's start day. Not applied to a `--dates-file` list unless given explicitly. |
| `--no-from-date` | Off | Submit every date (overrides `from_date` in `dataset.toml`) |
| `--keep-truncated` | Off | Also submit windows that end after the dataset does (by default they are skipped and counted) |
| `--commit REF` | Working tree | Benchmark this git commit, branch or tag |
| `--dataset-config PATH` | `dataset.toml` in the repo root | Config frozen into the snapshot |
| `--use-slurm` / `--no-use-slurm` | Off | Use Slurm instead of the local Task Spooler |
| `--route NAME` | `default` in `clusters.toml` | Slurm route from the clusters file; needed with `--use-slurm` |
| `--clusters-file PATH` | `benchmark/clusters.toml` | TOML describing the Slurm routes |
| `--exclude-nodes LIST` | Route's `exclude` list | Slurm nodes to exclude, replacing the route's `exclude` list |
| `--no-exclude` | Off | Exclude no nodes, overriding the above |
| `--debug` / `--no-debug` | Off | Ignore the date list and run one date, 2025-02-15, for a smoke test |
| `--graph-mode` | `no-graph` | `no-graph` or `save-graph` (which creates graphs per date and span, overwriting existing ones if they are present) |
| `--dump-periods-dir DIR` | None | Pass `--dump-periods` to every run, one `<date>_<span>d.json` per run in this folder (it must be writable from the nodes). Arms on the same date and span overwrite each other, so use one folder per arm. |

#### Flags passed straight through to `process.py` (see its section for more information)

| Flag | Default |
|---|---|
| `--head-weight`, `--head-fraction` | `process.py`'s (4, 0.25) |
| `--max-wait` | None |
| `--reschedule-percent`, `--reschedule-seed` | 100, 0 |
| `--defer` / `--no-defer`, `--retry-timeout` | Defer on, 72h |
| `--count-violations` / `--no-count-violations` | Off |
| `--warmup-days`, `--cooldown-days` | 10; the retry timeout in days with defer (3 at 72h), otherwise 0 |

A window is submitted for each date, so a 7-day sweep gives overlapping windows (one starting every day). Windows that would run past the end of the dataset are skipped, because they would see only the jobs that remain and look like ordinary results.

### `clusters.toml`: where the Slurm jobs run

Needed only with `--use-slurm`. Copy `benchmark/clusters.example.toml` to `benchmark/clusters.toml` (git-ignored). It is separate from `dataset.toml`: that describes the dataset, this says where the benchmark runs. A top-level `default = "name"` picks the route when `--route` is not given. Each `[routes.NAME]` block holds:

| Key | Meaning |
|---|---|
| `account`, `partition` | `sbatch -A` and `-p` |
| `qos` | QOS for any resources row that has none of its own |
| `reservation` | Optional, `sbatch --reservation` |
| `exclude` | Optional list of nodes to avoid |
| `max_time` | Optional wall-time cap of the cluster; a resources row above it is rejected when the file loads |
| `ends` | Optional timestamp (for example a reservation's end); warns when a job's time limit would run past it |
| `extra_args` | Optional raw `sbatch` arguments appended as given |
| `resources` | List of `{ max_days, mem, time, qos }` rows |

`resources` sets `--mem`, `-t` and the QOS per run. The first row's `max_days` that is big enough to hold the run's **streamed days** (span + warm-up + cool-down, so a 1-day run at the defaults is 14) is used. A row with no `max_days` matches anything; without one, a longer run is refused before anything is submitted. Routes can share an account and partition and differ only in QOS or reservation. Experiment with choosing rows: the memory and time limits in the example file are arbitrary examples, which will likely require tuning by running and measuring on your cluster.

### `benchmark/process.py`: logs to CSV

```bash
cd benchmark
uv run process.py
```

It takes no arguments. It reads every `*.txt` in `temp/` and writes one CSV per log to `processed/`, named after the log plus a hash of its contents. The columns are taken from whatever fields the log's rows contain, so new fields in `process.py`'s JSON appear automatically, and the settings each run used (`max_wait`, `reschedule_percent`, `defer`, `warmup_days`, and so on) are read from the command line stored in the log and added as extra columns, so every row says which setting produced it

Each row also gets an `error_type` when the run did not give a result: `timeout`, `cancelled`, `node_fail`, `out_of_memory`, `time_binary_missing`, `still_running`, `missing_carbon_data` or `virtual_capacity_exhausted`. It stops on a traceback it cannot classify rather than guess. Empty days (`num_jobs == 0`) are not failures.

If `runmultiple.py` died before writing its log, `recover.py` rebuilds logs from `slurmoutputs/job_*.log` plus `sacct`. They are written as `temp/<timestamp>-recovered-<n>.txt` with no commit, route or flag suffix, so rename each one using the command the script prints beside it.

### `compare.py`: compare two sweeps

```bash
cd benchmark
uv run compare.py processed/baseline.csv processed/candidate.csv
```

It prints one table per window length diffing success rate, runtime, peak RAM and carbon saving. Success rate uses every usable run on each side; everything else uses only the `(date, span)` runs that **both** CSVs completed, so a timeout on one side cannot change which days the other is judged on. Savings are shown as a per-day mean (dominated by small days) and carbon-weighted, `(Σcarbon − Σcarbon_2) / Σcarbon`, which is the figure to quote.

| Flag | Meaning |
|---|---|
| `--from-date YYYY-MM-DD` | Keep only runs whose window starts on or after this date. There is no default here, so compare sweeps with the same `--from-date` you ran them with. |
| `--keep-truncated` | Keep windows that run past the dataset end (dropped by default) |
| `--pooled` | Also print one table over all spans (mixes window lengths) |
| `--non-overlapping` | Keep only runs whose windows share no days, so runs are more independent but fewer (about 1/N at an N-day span). Which slice you keep moves the answer, so `compare.py` also prints the gain across the slices. |
