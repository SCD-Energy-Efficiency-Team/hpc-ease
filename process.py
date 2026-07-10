from dotenv import dotenv_values
from fetchcarbon import get_rounded_iso_range, get_climate_data
from datetime import datetime, timezone, timedelta
import matplotlib.pyplot as plt
import mariadb
from collections import defaultdict
from scheduling import CarbonScheduler
from tqdm import tqdm
import random

DURATION_TO_FETCH = timedelta(days=1)

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
    time_dict["total"] = row.pop("total_cpu_time")
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

def get_jobs(start_window, end_window):
    creds = dotenv_values(".env")
    conn = mariadb.connect(unix_socket="/var/lib/mysql/mysql.sock", user=creds["DB_USERNAME"], password=creds["DB_PASSWORD"], database=creds["DB_DATABASE"])
    with conn:
        with conn.cursor(dictionary=True) as cursor:
            query = f"""
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
                    CAST(SUM(step_table.user_sec + step_table.sys_sec + (step_table.sys_usec / 1000000) + (step_table.user_usec / 1000000)) AS DECIMAL(18, 6)) AS total_cpu_time
                FROM 
                    {creds["JOB_TABLE"]} job_table
                JOIN 
                    {creds["STEP_TABLE"]} step_table ON job_table.job_db_inx = step_table.job_db_inx
                WHERE
                    job_table.time_end >= %s AND job_table.time_end <= %s AND job_table.time_start >= %s AND job_table.state = 3
                GROUP BY 
                    job_table.job_db_inx
                ORDER BY 
                    job_table.time_submit ASC;
            """

            cursor.execute(query, (start_window, end_window, start_window))

            for row in cursor:
                yield job_row_to_dict(row)

def calc_energy_of_job(job, p_idle=220, p_max=850):
    elapsed_time = job["time"]["elapsed"]  # wall clock time
    cpu_time = float(job["time"]["total"])  # total cpu time

    cpus_total = job["alloc"]["cpu"] # total number of CPUs allocated (not per node)
    num_nodes = job["alloc"]["node"]  # number of nodes requested
    
    if elapsed_time == 0:
        # wall clock time is 0, so the job did nothing
        return 0

    # average CPU utilization across the cluster
    u = cpu_time / (elapsed_time * cpus_total)
    u = max(0.0, min(u, 1.0))

    # single-node equivalent power
    node_power = p_idle + u * (p_max - p_idle)

    # total cluster power used by job
    total_power = num_nodes * node_power

    energy_kwh = (total_power * (elapsed_time / 3600)) / 1000
    # joules = total_power * elapsed_time
    return energy_kwh

def get_jobs_and_energy(chosen_day):
    start_time = chosen_day.timestamp()
    end_time = (chosen_day + DURATION_TO_FETCH).timestamp()

    jobs = {}
    min_start, max_end = float("inf"), float("-inf")
    for job in get_jobs(start_time, end_time):
        state, start, end = job["state"], job["time"]["start"], job["time"]["end"]
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

def get_jobs_and_energy_reschedule(chosen_day):
    start_time = chosen_day.timestamp()
    end_time = (chosen_day + DURATION_TO_FETCH).timestamp()

    groups = defaultdict(list)
    for job in get_jobs(start_time, end_time):
        submit_time = datetime.fromtimestamp(job["time"]["submit"])
        submit_group = (submit_time.year, submit_time.month, submit_time.day, submit_time.hour, submit_time.minute)
        groups[submit_group].append(job)
    scheduler = CarbonScheduler()
    for key in tqdm(sorted(groups)):
        scheduler.process_jobs(groups[key])

    jobs = {}
    min_start, max_end = float("inf"), float("-inf")
    for job in scheduler.output_jobs():
        state, start, end = job["state"], job["time"]["start"], job["time"]["end"]
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

def accum_job_over_window(jobs, startv, endv):
    starttsv, endtsv = startv.timestamp(), endv.timestamp()
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
        energy_total, jobs_total = accum_job_over_window(jobs, start, end)
        
        chosen_carbon_intensity = None
        for carbonstart, carbonend in carbon_intensity:
            if not (end <= carbonstart or start >= carbonend):
                chosen_carbon_intensity = carbon_intensity[(carbonstart, carbonend)]
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

def draw_graphs(periods, periods2=None):
    times = [p["time"] for p in periods]
    if periods2 is not None:
        times2 = [p["time"] for p in periods2]

    fig, axes = plt.subplots(2, 2, figsize=(10, 8))
    ax1, ax2, ax3, ax4 = axes.flatten()

    ax1.plot(times, [p["energy"] for p in periods])
    if periods2 is not None:
        ax1.plot(times2, [p["energy"] for p in periods2])
    ax1.set_title("Energy (kWh)")

    ax2.plot(times, [p["jobs"] for p in periods])
    if periods2 is not None:
        ax2.plot(times2, [p["jobs"] for p in periods2])
    ax2.set_title("Number of jobs")

    ax3.plot(times, [p["intensity"] for p in periods])
    if periods2 is not None:
        ax3.plot(times2, [p["intensity"] for p in periods2])
    ax3.set_title("Grid carbon intensity (gCO₂/kWh)")

    ax4.plot(times, [p["carbon"] for p in periods])
    if periods2 is not None:
        ax4.plot(times2, [p["carbon"] for p in periods2])
    ax4.set_title("Job carbon impact (kgCO₂)")

    plt.tight_layout()
    plt.show()

def general_stats(periods, custom_comment=""):
    energy = sum(period["energy"] for period in periods)
    carbon = sum(period["carbon"] for period in periods)
    print(f"Total energy {custom_comment}: {energy:.3f} kWh")
    print(f"Total carbon {custom_comment}: {carbon:.3f} kgCO₂")

if __name__ == "__main__":
    choose_random = False
    if choose_random:
        random_timestamp = random.randint(1725608400, 1780351201)
        random_date = datetime.fromtimestamp(random_timestamp, tz=timezone.utc)
        random_date = random_date.replace(hour=0, minute=0, second=0, microsecond=0)
        print(random_date)
        chosen_day = random_date
    else:
        chosen_day = datetime(2025, 11, 9, tzinfo=timezone.utc)
        # chosen_day = datetime(2026, 5, 4, tzinfo=timezone.utc)
    
    jobs, min_start, max_end = get_jobs_and_energy(chosen_day)
    print(f"Total number of jobs: {sum(len(group) for group in jobs.values())}")
    periods = get_graph_data(jobs, min_start, max_end)

    compare_algorithm = True
    if compare_algorithm:
        jobs2, min_start2, max_end2 = get_jobs_and_energy_reschedule(chosen_day)
        periods2 = get_graph_data(jobs2, min_start2, max_end2)
        draw_graphs(periods, periods2)
        general_stats(periods, "prior")
        general_stats(periods2, "after")
    else:
        draw_graphs(periods)
        general_stats(periods)