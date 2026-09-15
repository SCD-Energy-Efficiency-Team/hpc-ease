import faulthandler
import json
import logging
import math
import random
import signal
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path

import typer
from fetchcarbon import CarbonIntensityCSV as CarbonIntensityService
from fetchcarbon import get_rounded_iso_range
from fetchjobs import get_jobs_from_db, get_jobs_from_parquet
from scheduling import CarbonScheduler

logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

faulthandler.register(signal.SIGQUIT)

app = typer.Typer()

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

def aggregate_jobs(jobs_iter):
    jobs = defaultdict(list)
    min_start, max_end = float("inf"), float("-inf")
    for job in jobs_iter:
        state, start, end = job["state"], job["time"]["start"], job["time"]["end"]
        assert start != 0 and end != 0, f"{job['id_job']} {state} {start} {end}"
        min_start = min(min_start, start)
        max_end = max(max_end, end)
        jobs[(start, end)].append(calc_energy_of_job(job))
    return dict(jobs), min_start, max_end

def get_jobs_and_energy(start_time, end_time, by_submit_time, get_jobs, jobs_file):
    return aggregate_jobs(get_jobs(start_time, end_time, jobs_file, by_submit_time))

def get_jobs_and_energy_reschedule(start_time, end_time, by_submit_time, get_jobs, jobs_file, cidservice):
    scheduler = CarbonScheduler(cidservice)
    current = start_time
    while current < end_time:
        jobs = get_jobs(current, current + 60, jobs_file, by_submit_time)
        scheduler.process_jobs(jobs)
        current += 60

    horizon_stats = {
        "unschedulable_jobs": len(scheduler.unschedulable),
        "unschedulable_energy": sum(calc_energy_of_job(job) for job in scheduler.unschedulable),
    }
    return (*aggregate_jobs(scheduler.output_jobs()), horizon_stats)

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

def get_graph_data(jobs, min_start, max_end, cidservice):
    limitstart, limitend = get_rounded_iso_range(min_start, max_end)
    carbon_intensity = cidservice.get_climate_data(limitstart, limitend, "OX11", "forecast")

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

def draw_graphs(cidservice, periods, periods2=None, num_jobs=None, save_image=True, filename=None, title=""):
    import matplotlib.pyplot as plt

    times = [p["time"] for p in periods]
    if periods2 is not None:
        times2 = [p["time"] for p in periods2]

    fig, axes = plt.subplots(2, 2, figsize=(12, 6))
    ax1, ax2, ax3, ax4 = axes.flatten()

    before_color = "tab:red"
    after_color = "tab:green"
    neutral_color = "tab:blue"

    ax1.plot(times, [p["energy"] for p in periods], color=before_color)
    if periods2 is not None:
        ax1.plot(times2, [p["energy"] for p in periods2], color=after_color)
    ax1.set_title("Energy (kWh)")

    ax2.plot(times, [p["jobs"] for p in periods], color=before_color)
    if periods2 is not None:
        ax2.plot(times2, [p["jobs"] for p in periods2], color=after_color)
    num_jobs = f" ({num_jobs})" if num_jobs is not None else ""
    ax2.set_title("Number of jobs" + num_jobs)

    alltimes = times + times2
    limitstart, limitend = min(alltimes), max(alltimes)
    grid_intensity = cidservice.get_climate_data(*get_rounded_iso_range(limitstart.timestamp(), limitend.timestamp()))
    grid_intensity_xs = [x[0] for x in grid_intensity]
    grid_intensity_ys = list(grid_intensity.values())
    ax3.plot(grid_intensity_xs, grid_intensity_ys, color=neutral_color)
    ax3.set_title("Grid carbon intensity (gCO₂/kWh)")

    ax4.plot(times, [p["carbon"] for p in periods], color=before_color)
    if periods2 is not None:
        ax4.plot(times2, [p["carbon"] for p in periods2], color=after_color)
    ax4.set_title("Job carbon impact (kgCO₂)")

    fig.autofmt_xdate()
    plt.tight_layout(rect=[0, 0.075, 1, 0.95])
    fig.suptitle(title, fontsize=16, fontweight="bold", y=0.98)
    subtitle = "\n".join(general_stats(periods, periods2))
    fig.text(0.5, 0.02, subtitle, ha="center", fontweight="bold", fontsize=12)

    if save_image:
        if filename is None:
            filename = f"{int(datetime.now(UTC).timestamp() * 1000)}.png"
        plt.savefig(filename, dpi=200)
    else:
        plt.show()

def general_stats(periods, periods2=None, return_json=False):
    # output is either "print", "text" (returns text) or "json" (returns json)
    energy_2, carbon_2 = False, False
    if periods2 is None:
        energy = sum(period["energy"] for period in periods)
        carbon = sum(period["carbon"] for period in periods)
        first_text = f"Total energy: {energy:.3f} kWh"
        second_text = f"Total carbon: {carbon:.3f} kgCO₂"
    else:
        energy = sum(period["energy"] for period in periods)
        energy_2 = sum(period["energy"] for period in periods2)
        carbon = sum(period["carbon"] for period in periods)
        carbon_2 = sum(period["carbon"] for period in periods2)
        if math.isclose(energy, energy_2):
            first_text = f"Total energy: {energy:.3f} kWh"
        else:
            first_text = f"Energy change: {energy:.3f} kWh → {energy_2:.3f} kWh ({energy_2 - energy:+.3f} kWh)"
        second_text = f"Carbon change: {carbon:.3f} kgCO₂ → {carbon_2:.3f} kgCO₂ ({carbon_2 - carbon:+.3f} kgCO₂)"
    
    if return_json:
        data = {"energy": energy, "carbon": carbon}
        if periods2 is not None:
            data |= {"energy_2": energy_2, "carbon_2": carbon_2}
        return data
    else:
        return first_text, second_text

def get_filename_and_title(chosen_day, days_to_fetch, by_submit_time, compare_algorithm):
    filename = [str(chosen_day.date()), f"{days_to_fetch}d"]
    if not by_submit_time:
        filename.append("notbysubmit")
    if compare_algorithm:
        filename.append("compare")
    Path("../graphs").mkdir(parents=True, exist_ok=True)
    filename = f"../graphs/{'_'.join(filename)}.png"
    title = [str(chosen_day.date()), f"{days_to_fetch} day{'s' if days_to_fetch != 1 else ''} ahead"]
    title = ", ".join(title)
    return filename, title

class GraphMode(str, Enum):
    no_graph = "no-graph"
    save_graph = "save-graph"
    show_graph = "show-graph"

@app.command()
def run_main(
    chosen_day: datetime | None = None,
    days_to_fetch: int = 1,
    by_submit_time: bool = True,
    compare_algorithm: bool = True,
    graph_mode: GraphMode = GraphMode.save_graph,
    print_json: bool = False,
    do_ask: bool = True,
    jobs_file: str | None = None
):
    # 2025-11-9 (16373), 2026-5-4 (89424), 2025-1-9 (7850), 2025-2-15 (2344)
    choose_random = chosen_day is None
    duration_to_fetch = timedelta(days=days_to_fetch)
    if chosen_day is not None:
        chosen_day = chosen_day.replace(tzinfo=UTC)

    cidservice = CarbonIntensityService()

    print(" ".join(sys.argv))

    if jobs_file:
        get_jobs = get_jobs_from_parquet
    else:
        get_jobs = get_jobs_from_db

    while True:
        if choose_random:
            random_timestamp = random.randint(1725608400, 1780351201)
            random_date = datetime.fromtimestamp(random_timestamp, tz=UTC)
            random_date = random_date.replace(hour=0, minute=0, second=0, microsecond=0)
            logger.info(f"Chosen random date: {random_date}")
            chosen_day = random_date
        
        logger.info("Getting jobs...")
        start_time = chosen_day.timestamp()
        end_time = (chosen_day + duration_to_fetch).timestamp()
        jobs, min_start, max_end = get_jobs_and_energy(start_time, end_time, by_submit_time, get_jobs, jobs_file)
        num_jobs = sum(len(group) for group in jobs.values())
        logger.info(f"Total number of jobs: {num_jobs}")
        if not print_json and do_ask:
            input("Press enter to continue...")
        if num_jobs == 0:
            if choose_random:
                continue
            else:
                break
        periods = get_graph_data(jobs, min_start, max_end, cidservice)
        break

    graph_filename, title = get_filename_and_title(chosen_day, days_to_fetch, by_submit_time, compare_algorithm)
    save_image = graph_mode == GraphMode.save_graph

    result_json = {"num_jobs": num_jobs, "chosen_day": chosen_day.isoformat(), "days_to_fetch": days_to_fetch}
    if num_jobs == 0:
        print(json.dumps(result_json))
        sys.exit()
    if compare_algorithm:
        logger.info("Running alternative scheduling algorithm...")
        jobs2, min_start2, max_end2, horizon_stats = get_jobs_and_energy_reschedule(start_time, end_time, by_submit_time, get_jobs, jobs_file, cidservice)
        periods2 = get_graph_data(jobs2, min_start2, max_end2, cidservice)
        if horizon_stats["unschedulable_jobs"]:
            energy_frac = 100 * horizon_stats["unschedulable_energy"] / sum(period["energy"] for period in periods)
            logger.info(
                f"{horizon_stats['unschedulable_jobs']} job(s) longer than the carbon forecast "
                f"horizon were left at their original start ({energy_frac:.2f}% of energy)"
            )
        result_json |= horizon_stats
    else:
        periods2 = None
    if graph_mode != GraphMode.no_graph:
        draw_graphs(cidservice, periods, periods2, num_jobs=num_jobs, save_image=save_image, filename=graph_filename, title=title)
    stats = general_stats(periods, periods2, return_json=print_json)
    
    if not print_json:
        print("\n".join(stats))
    else:
        result_json |= stats
        print(json.dumps(result_json))

if __name__ == "__main__":
    app()