## Setup

You can install dependencies using [uv](https://docs.astral.sh/uv/): `uv sync`

## Use

Currently the scripts requires direct access to the database behind the sacct command, the main script is `process.py`. As of now, it processes a selected day's worth of data from the database, estimates the power usage of each job, combines that with carbon intensity data fetched from the [UK's Carbon Intensity API](https://carbonintensity.org.uk/) and combines everything together to produce four graphs: energy (kWh), the number of jobs, the carbon intensity of the grid (gCO₂/kWh) and the carbon intensity of the jobs (kgCO₂) over the period.