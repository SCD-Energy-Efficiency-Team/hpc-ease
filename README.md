## Setup

You can install dependencies using [uv](https://docs.astral.sh/uv/): `uv sync`

## Use

1. Currently the scripts use a JSON export from sacct. You can get one using a command like so:
`sacct --allusers --state=COMPLETED -S 05/04-00:00 -E 05/05-01:00 --json -X > may4th2026.json`

2. Place the exported file in `prodjobs/raw`, then run `preprocessraw.py`, this will create a line delimitted (`.jsonl`) file in `prodjobs\lists`

3. Set the variable `jobs_filename` to the path of the jobs file (e.g. `jobs_filename = "prodjobs/lists/may4th2026.jsonl"`) in `process.py`, then run `process.py`

Currently the script processes the jobs file, estimates the power usage of each job, combines that with carbon intensity data fetched from the [UK's Carbon Intensity API](https://carbonintensity.org.uk/) and combines everything together to produce four graphs: energy (kWh), the number of jobs, the carbon intensity of the grid (gCO₂/kWh) and the carbon intensity of the jobs (kgCO₂) over the period.