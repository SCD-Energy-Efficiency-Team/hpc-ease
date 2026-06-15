import json
from tqdm import tqdm
from fetchcarbon import get_rounded_iso_range, get_climate_data
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import random

def get_jobs(filename, do_random=True, show_progress=True):
    line_offsets = []

    with open(filename, "rb") as file:
        while True:
            offset = file.tell()
            if not file.readline():
                break
            line_offsets.append(offset)

    if do_random:
        random.shuffle(line_offsets)

    with open(filename, "r", encoding="utf-8") as file:
        for offset in tqdm(line_offsets):
            file.seek(offset)
            yield json.loads(file.readline().strip())

def calc_energy_of_job(job, p_idle=220, p_max=850):
    elapsed_time = job["time"]["elapsed"] # wall clock time
    cpu_time = job["time"]["total"] # total cpu time
    cpu_time = cpu_time["seconds"] + (cpu_time["microseconds"] / 10**6)

    # elapsed_time is rounded to the nearest second, and multi-core jobs are likely to use more
    # than elapsed_time so cpu_time can overshoot
    if cpu_time > elapsed_time:
        elapsed_time = cpu_time
    # you get a division by zero error otherwise
    if elapsed_time == 0:
        assert cpu_time == 0
        cpu_time = 1
        elapsed_time = 1

    tres_allocated = {item["type"]: item["count"] for item in job["tres"]["allocated"]}
    nodes = tres_allocated["node"] # number of nodes requested
    cpus_total = tres_allocated["cpu"] # total number of CPUs allocated (not per node)

    # average CPU utilization across the job
    u = cpu_time / (elapsed_time * cpus_total)
    u = max(0.0, min(u, 1.0))

    node_power = p_idle + u * (p_max - p_idle)
    total_power = nodes * node_power
    
    energy_kwh = (total_power * (elapsed_time / 3600)) / 1000
    # return total_power * elapsed_time # joules

    return energy_kwh

jobs_filename = "prodjobs/lists/may4th2026.jsonl"

jobs = {}
min_start, max_end = float("inf"), float("-inf")
for job in get_jobs(jobs_filename):
    state, start, end = job["state"]["current"][0], job["time"]["start"], job["time"]["end"]
    if state == "COMPLETED":
        assert start != 0 and end != 0, f"{job['job_id']} {state} {start} {end}"
        if start < min_start:
            min_start = start
        if end > max_end:
            max_end = end
        energy = calc_energy_of_job(job)
        timerange = (start, end)
        if timerange not in jobs:
            jobs[timerange] = []
        jobs[timerange].append(energy)

limitstart, limitend = get_rounded_iso_range(min_start, max_end)

carbon_intensity = get_climate_data("2026-06-02T00:30Z", "2026-06-04T10:30Z", "OX11", "forecast")

energy_by_period = {}
jobs_by_period = {}
carbon_intensity_by_period = {}
interval_mins = 5
limitstart, limitend = datetime.fromisoformat(limitstart), datetime.fromisoformat(limitend)
start = limitstart
while True:
    end = start + timedelta(minutes=interval_mins)
    startts, endts = start.timestamp(), end.timestamp()

    jobs_by_period[endts] = 0
    subtotal = 0

    for (job_start, job_end), energies in jobs.items():
        job_duration = job_end - job_start

        overlap_start = max(startts, job_start)
        overlap_end = min(endts, job_end)

        if overlap_end <= overlap_start:
            continue

        jobs_by_period[endts] += 1

        overlap = overlap_end - overlap_start
        fraction = overlap / job_duration

        for energy in energies:
            subtotal += energy * fraction
    
    energy_by_period[endts] = subtotal

    chosen_carbon_intensity = None
    for carbonstartts, carbonendts in carbon_intensity:
        startinrange = startts >= carbonstartts and startts <= carbonendts
        endinrange = endts >= carbonstartts and endts <= carbonendts
        if startinrange == endinrange:
            chosen_carbon_intensity = carbon_intensity[(carbonstartts, carbonendts)]
    carbon_intensity_by_period[endts] = (subtotal * chosen_carbon_intensity) / 1000

    if endts >= limitend.timestamp():
        break
    start += timedelta(minutes=interval_mins)

carbon_intensity_for_graph = {datetime.fromtimestamp(timerange[0]): value for timerange, value in carbon_intensity.items()}
cifg_keys = sorted(carbon_intensity_for_graph)

keys = sorted(energy_by_period)
xkeys = [datetime.fromtimestamp(key) for key in keys]

fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(10, 8))
ax1, ax2, ax3, ax4 = axes.flatten()

ax1.plot(xkeys, [energy_by_period[key] for key in keys])
ax1.set_title("Energy (kWh)")

ax2.plot(xkeys, [jobs_by_period[key] for key in keys])
ax2.set_title("Number of jobs")

ax3.plot(cifg_keys, [carbon_intensity_for_graph[key] for key in cifg_keys])
ax3.set_title("Carbon intensity of grid (gCO₂/kWh)")

ax4.plot(xkeys, [carbon_intensity_by_period[key] for key in keys])
ax4.set_title("Carbon intensity of jobs (kgCO₂)")

plt.tight_layout()
plt.show()