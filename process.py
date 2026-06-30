from dotenv import dotenv_values
from fetchcarbon import get_rounded_iso_range, get_climate_data
from datetime import datetime, timezone, timedelta
import matplotlib.pyplot as plt
import mariadb

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
    time_dict["start"] = row["time_start"]
    time_dict["end"] = row["time_end"]
    time_dict["elapsed"] = row["time_end"] - row["time_start"]
    time_dict["total"] = row["total_cpu_time"]
    row_dict = {"time": time_dict}
    row_dict["state"] = STATES[row["state"]]
    for line in row["tres_alloc"].split(","):
        key, value = tuple(map(int, line.split("=")))
        row_dict["tres_"+TRES_KEYS[key]] = value
    row_dict["job_db_inx"] = row["job_db_inx"]
    return row_dict

def get_jobs(start_window, end_window):
    creds = dotenv_values(".env")
    conn = mariadb.connect(user=creds["DB_USERNAME"], password=creds["DB_PASSWORD"], database=creds["DB_DATABASE"])
    with conn:
        with conn.cursor(dictionary=True) as cursor:
            query = f"""
                SELECT
                    job_table.job_db_inx AS job_db_inx,  
                    job_table.id_job AS id_job,
                    job_table.state AS state,
                    job_table.time_start AS time_start,
                    job_table.time_end AS time_end,
                    job_table.tres_alloc AS tres_alloc,
                    CAST(SUM(step_table.user_sec + step_table.sys_sec + (step_table.sys_usec / 1000000) + (step_table.user_usec / 1000000)) AS DECIMAL(18, 6)) AS total_cpu_time
                FROM 
                    {creds["JOB_TABLE"]} job_table
                JOIN 
                    {creds["STEP_TABLE"]} step_table ON job_table.job_db_inx = step_table.job_db_inx
                WHERE
                    job_table.time_end >= %s AND job_table.time_end <= %s AND job_table.time_start >= %s AND job_table.state = 3
                GROUP BY 
                    job_table.job_db_inx;
            """

            cursor.execute(query, (start_window, end_window, start_window))

            for row in cursor:
                yield job_row_to_dict(row)

def calc_energy_of_job(job, p_idle=220, p_max=850):
    elapsed_time = job["time"]["elapsed"] # wall clock time
    cpu_time = float(job["time"]["total"]) # total cpu time

    # elapsed_time is rounded to the nearest second, and multi-core jobs are likely to use more
    # than elapsed_time so cpu_time can overshoot
    if cpu_time > elapsed_time:
        elapsed_time = cpu_time
    # you get a division by zero error otherwise
    if elapsed_time == 0:
        assert cpu_time == 0
        cpu_time = 1
        elapsed_time = 1

    nodes = job["tres_node"] # number of nodes requested
    cpus_total = job["tres_cpu"] # total number of CPUs allocated (not per node)

    # average CPU utilization across the job
    u = cpu_time / (elapsed_time * cpus_total)
    u = max(0.0, min(u, 1.0))

    node_power = p_idle + u * (p_max - p_idle)
    total_power = nodes * node_power
    
    energy_kwh = (total_power * (elapsed_time / 3600)) / 1000
    # return total_power * elapsed_time # joules

    return energy_kwh

def get_jobs_and_energy(chosen_day):
    start_time = chosen_day.timestamp()
    end_time = (chosen_day + timedelta(days=1)).timestamp()

    jobs = {}
    min_start, max_end = float("inf"), float("-inf")
    for job in get_jobs(start_time, end_time):
        state, start, end = job["state"], job["time"]["start"], job["time"]["end"]
        assert state == "COMPLETED", state # Not sure if needed
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
    return jobs, min_start, max_end

def iter_periods(startv, endv, minutes=5):
    step = timedelta(minutes=minutes)
    while startv < endv:
        yield startv, startv + step
        startv += step

def accum_job_over_window(jobs, starttsv, endtsv):
    jobs_total = 0
    energy_total = 0

    for (job_start, job_end), energies in jobs.items():
        job_duration = job_end - job_start

        overlap_start = max(starttsv, job_start)
        overlap_end = min(endtsv, job_end)

        if overlap_end <= overlap_start:
            continue

        jobs_total += 1

        overlap = overlap_end - overlap_start
        fraction = overlap / job_duration

        for energy in energies:
            energy_total += energy * fraction
    
    return energy_total, jobs_total

def get_graph_data(jobs, min_start, max_end):
    limitstart, limitend = get_rounded_iso_range(min_start, max_end)
    carbon_intensity = get_climate_data(limitstart, limitend, "OX11", "forecast")

    periods = []
    limitstart, limitend = datetime.fromisoformat(limitstart), datetime.fromisoformat(limitend)
    for start, end in iter_periods(limitstart, limitend):
        startts, endts = start.timestamp(), end.timestamp()

        energy_total, jobs_total = accum_job_over_window(jobs, startts, endts)
        
        chosen_carbon_intensity = None
        for carbonstartts, carbonendts in carbon_intensity:
            if not (endts <= carbonstartts or startts >= carbonendts):
                chosen_carbon_intensity = carbon_intensity[(carbonstartts, carbonendts)]
                break

        period = {
            "time": end,
            "energy": energy_total,
            "jobs": jobs_total,
            "intensity": chosen_carbon_intensity,
            "carbon": (energy_total * chosen_carbon_intensity) / 1000
        }

        periods.append(period)

    return periods

def draw_graphs(periods):
    times = [p["time"] for p in periods]

    fig, axes = plt.subplots(2, 2, figsize=(10, 8))
    ax1, ax2, ax3, ax4 = axes.flatten()

    ax1.plot(times, [p["energy"] for p in periods])
    ax1.set_title("Energy (kWh)")

    ax2.plot(times, [p["jobs"] for p in periods])
    ax2.set_title("Number of jobs")

    ax3.plot(times, [p["intensity"] for p in periods])
    ax3.set_title("Grid carbon intensity (gCO₂/kWh)")

    ax4.plot(times, [p["carbon"] for p in periods])
    ax4.set_title("Job carbon impact (kgCO₂)")

    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    chosen_day = datetime(2026, 5, 4, tzinfo=timezone.utc)
    jobs, min_start, max_end = get_jobs_and_energy(chosen_day)
    periods = get_graph_data(jobs, min_start, max_end)
    draw_graphs(periods)