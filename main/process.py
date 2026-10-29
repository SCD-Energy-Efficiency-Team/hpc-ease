import faulthandler
import json
import logging
import math
import random
import signal
import sys
from bisect import bisect_left
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path

import typer
from fetchcarbon import (
    DEFAULT_HEAD_FRACTION,
    DEFAULT_HEAD_WEIGHT,
    get_rounded_iso_range,
)
from fetchcarbon import CarbonIntensityCSV as CarbonIntensityService
from fetchjobs import (
    BATCH_SECONDS,
    get_job_batches_from_db,
    get_job_batches_from_parquet,
    get_jobs_from_db,
    get_jobs_from_parquet,
)
from scheduling import CarbonScheduler
from tqdm import tqdm

logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

faulthandler.register(signal.SIGQUIT)

# keep equal to benchmark/runmultiple.py's DEFAULT_WARMUP_DAYS
DEFAULT_WARMUP_DAYS = 10.0

app = typer.Typer()

NODE_CORES = 192  # 2 sockets x 96 cores per node

def calc_energy_of_job(job, p_idle=220, p_max=850):
    elapsed_time = job["time"]["elapsed"]  # wall clock time
    cpu_time = float(job["time"]["total"])  # total cpu time

    cpus_total = job["alloc"]["cpu"] # total number of CPUs allocated (not per node)

    if elapsed_time == 0:
        # wall clock time is 0, so the job did nothing
        return 0

    # average CPU utilization across the cluster
    u = cpu_time / (elapsed_time * cpus_total)
    u = max(0.0, min(u, 1.0))

    # single-node equivalent power
    node_power = p_idle + u * (p_max - p_idle)

    # total cluster power used by job
    total_power = (cpus_total / NODE_CORES) * node_power

    energy_kwh = (total_power * (elapsed_time / 3600)) / 1000
    return energy_kwh

def aggregate_jobs(jobs_iter, waits=None):
    jobs = defaultdict(list)
    min_start, max_end = float("inf"), float("-inf")
    for job in jobs_iter:
        state, start, end = job["state"], job["time"]["start"], job["time"]["end"]
        assert start != 0 and end != 0, f"{job['id_job']} {state} {start} {end}"
        min_start = min(min_start, start)
        max_end = max(max_end, end)
        jobs[(start, end)].append(calc_energy_of_job(job))
        if waits is not None:
            waits.append(start - job["time"]["submit"])
    return dict(jobs), min_start, max_end

def summarise_waits(waits, suffix=""):
    ordered = sorted(waits)
    last = len(ordered) - 1
    return {
        f"wait_median{suffix}": ordered[last // 2],
        f"wait_p90{suffix}": ordered[round(0.9 * last)],
        f"wait_p95{suffix}": ordered[round(0.95 * last)],
        f"wait_p99{suffix}": ordered[round(0.99 * last)],
        f"wait_max{suffix}": ordered[-1],
        f"wait_negative{suffix}": sum(1 for w in ordered if w < 0),
    }

def get_jobs_and_energy(start_time, end_time, by_submit_time, get_jobs, jobs_file, waits=None):
    return aggregate_jobs(get_jobs(start_time, end_time, jobs_file, by_submit_time), waits)

def aggregate_energies(placements):
    jobs = defaultdict(list)
    min_start, max_end = float("inf"), float("-inf")
    for start, end, energy in placements:
        assert start != 0 and end != 0, f"{start} {end}"
        min_start = min(min_start, start)
        max_end = max(max_end, end)
        jobs[(start, end)].append(energy)
    return dict(jobs), min_start, max_end

def get_jobs_and_energy_reschedule(start_time, end_time, by_submit_time, get_job_batches, jobs_file, cidservice, strict=False, reschedule_fraction=1.0, reschedule_seed=0,
                                   defer=False, retry_timeout=24.0, count_violations=False,
                                   warmup_days=0.0, cooldown_days=0.0):
    scheduler = CarbonScheduler(cidservice, strict=strict, energy_fn=calc_energy_of_job, reschedule_fraction=reschedule_fraction, reschedule_seed=reschedule_seed,
                                defer=defer, retry_timeout=timedelta(hours=retry_timeout), count_violations=count_violations,
                                measure_from=start_time if warmup_days else None, measure_until=end_time if cooldown_days else None)
    stream_start = start_time - warmup_days * 86400
    stream_end = end_time + cooldown_days * 86400
    num_batches = math.ceil((int(stream_end) - int(stream_start)) / BATCH_SECONDS)  # CHUNK_SECONDS is a multiple of BATCH_SECONDS
    batches = tqdm(get_job_batches(stream_start, stream_end, jobs_file, by_submit_time), total=num_batches, desc="Rescheduling", unit="min", disable=None)
    for batch_start_epoch, batch in batches:
        scheduler.process_jobs(batch, batch_start_epoch)
    scheduler.finalize()

    horizon_stats = {
        "unschedulable_jobs": len(scheduler.unschedulable),
        "unschedulable_energy": sum(calc_energy_of_job(job) for job in scheduler.unschedulable),
        "crowded_out_jobs": len(scheduler.crowded_out),
        "crowded_out_energy": sum(calc_energy_of_job(job) for job in scheduler.crowded_out),
        "left_alone_jobs": len(scheduler.left_alone),
    }
    if defer:
        horizon_stats |= {
            "deferred_jobs": len(scheduler.deferred),
            "deferred_placed": len(scheduler.deferred) - len(scheduler.never_placed),
            "never_placed_jobs": len(scheduler.never_placed),
            "never_placed_energy": sum(calc_energy_of_job(job) for job in scheduler.never_placed),
        }
    if count_violations:
        unschedulable_ids = {id(j) for j in scheduler.unschedulable}
        violating_unschedulable = [j for j in scheduler.violating_pins if id(j) in unschedulable_ids]
        horizon_stats |= {
            "violating_pin_jobs": len(scheduler.violating_pins),
            "violating_pin_energy": sum(calc_energy_of_job(job) for job in scheduler.violating_pins),
            "violating_pin_jobs_unschedulable": len(violating_unschedulable),
            "violating_pin_energy_unschedulable": sum(calc_energy_of_job(job) for job in violating_unschedulable),
            "unverifiable_pin_jobs": len(scheduler.unverifiable_pins),
            "unverifiable_pin_energy": sum(calc_energy_of_job(job) for job in scheduler.unverifiable_pins),
        }
    waits = list(scheduler.output_waits())
    if waits:
        horizon_stats |= summarise_waits(waits, "_2")
    placed_waits = list(scheduler.output_placed_waits())
    if placed_waits:
        horizon_stats |= summarise_waits(placed_waits, "_placed_2")
    return (*aggregate_energies(scheduler.output_energies()), horizon_stats)

def iter_periods(startv, endv, minutes=5):
    step = timedelta(minutes=minutes)
    while startv < endv:
        yield startv, startv + step
        startv += step

def accum_jobs_over_periods(jobs, start_ts, num_periods, step=300):
    # Spreads each (start, end) group over its own periods once; returns per-period (energy, group count).
    energy = [0.0] * num_periods
    energy_full = [0.0] * (num_periods + 1)  # difference array: rate * step on fully covered periods
    count_diff = [0] * (num_periods + 1)
    for (job_start, job_end), energies in jobs.items():
        duration = job_end - job_start
        if duration <= 0:
            continue
        first = max(0, math.floor((job_start - start_ts) / step))
        last = min(num_periods - 1, math.ceil((job_end - start_ts) / step) - 1)
        if last < first:
            continue
        rate = sum(energies) / duration
        count_diff[first] += 1
        count_diff[last + 1] -= 1
        for i in {first, last}:
            period_start = start_ts + i * step
            overlap = min(job_end, period_start + step) - max(job_start, period_start)
            energy[i] += rate * overlap
        if last - first > 1:
            energy_full[first + 1] += rate * step
            energy_full[last] -= rate * step

    running, groups = 0.0, 0
    counts = []
    for i in range(num_periods):
        running += energy_full[i]
        groups += count_diff[i]
        if groups == 0:
            running = 0.0  # nothing covers this period: drop the difference array's float residue
        energy[i] += running
        counts.append(groups)
    return energy, counts

def get_graph_data(jobs, min_start, max_end, cidservice):
    limitstart, limitend = get_rounded_iso_range(min_start, max_end)
    if limitstart == limitend:
        # Zero-width range: the lookup loop never terminates on it.
        return []
    carbon_intensity = cidservice.get_climate_data(limitstart, limitend, "OX11", "forecast")

    limitstart, limitend = datetime.fromisoformat(limitstart), datetime.fromisoformat(limitend)
    period_bounds = list(iter_periods(limitstart, limitend))
    energies, counts = accum_jobs_over_periods(jobs, limitstart.timestamp(), len(period_bounds))

    slots = sorted(carbon_intensity.items())
    slot_starts = [slot_range[0] for slot_range, _ in slots]

    periods = []
    for (start, end), energy_total, jobs_total in zip(period_bounds, energies, counts):
        # slots don't overlap, so the only candidate is the last one starting before this period ends
        chosen_carbon_intensity = None
        idx = bisect_left(slot_starts, end) - 1
        if idx >= 0 and slots[idx][0][1] > start:
            chosen_carbon_intensity = slots[idx][1]

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

    alltimes = times + times2 if periods2 is not None else times
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

def dump_periods_json(path, cidservice, chosen_day, days_to_fetch, periods, periods2, horizon_stats):
    # the 5-minute series behind draw_graphs, for plotting later without rerunning the window
    def serialise(ps):
        return [{**p, "time": p["time"].isoformat()} for p in ps]

    lo = min(periods[0]["time"], periods2[0]["time"])
    hi = max(periods[-1]["time"], periods2[-1]["time"])
    grid = cidservice.get_climate_data(*get_rounded_iso_range(lo.timestamp(), hi.timestamp()))
    carbon = sum(p["carbon"] for p in periods)
    carbon_2 = sum(p["carbon"] for p in periods2)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "chosen_day": chosen_day.isoformat(),
        "days_to_fetch": days_to_fetch,
        "saving_pct": 100 * (carbon - carbon_2) / carbon,
        "periods": serialise(periods),
        "periods2": serialise(periods2),
        "grid": [(start.isoformat(), value) for (start, _), value in grid.items()],
        "stats": horizon_stats,
    }))
    logger.info(f"Saved periods to {path}")

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
    jobs_file: str | None = None,
    strict: bool = False,  # raise VirtualCapacityExhausted instead of pinning crowded-out jobs
    head_weight: float = DEFAULT_HEAD_WEIGHT,  # window-scoring weight on the first part of a window; 1.0 = flat
    head_fraction: float = DEFAULT_HEAD_FRACTION,
    max_wait: float | None = None,  # hours after submit a window may start; omit for 96h
    reschedule_percent: float = 100.0,  # percent of jobs moved; the rest keep their start
    reschedule_seed: int = 0,  # random subset picked by --reschedule-percent
    defer: bool = True,  # retry jobs with no free window as the forecast horizon advances; --no-defer pins them
    retry_timeout: float = 72.0,  # hours a deferred job may stay unplaced
    count_violations: bool = False,  # count pins that exceed node capacity (slower)
    warmup_days: float = DEFAULT_WARMUP_DAYS,  # unmeasured days run first; 0 for an empty start
    cooldown_days: float | None = None,  # unmeasured days run after; default is the retry timeout in days with --defer, 0 otherwise
    dump_periods: str | None = None,  # JSON file for the before/after 5-minute series
):
    choose_random = chosen_day is None
    duration_to_fetch = timedelta(days=days_to_fetch)
    if chosen_day is not None:
        chosen_day = chosen_day.replace(tzinfo=UTC)

    if cooldown_days is None:
        cooldown_days = retry_timeout / 24 if defer else 0.0
    if warmup_days < 0 or cooldown_days < 0:
        raise typer.BadParameter("--warmup-days and --cooldown-days must be >= 0")
    if not 0 <= reschedule_percent <= 100:
        raise typer.BadParameter("--reschedule-percent must be between 0 and 100")
    if dump_periods and not compare_algorithm:
        raise typer.BadParameter("--dump-periods needs the rescheduled run; drop --no-compare-algorithm")
    if dump_periods and (not warmup_days or (defer and not cooldown_days)):
        logger.warning("--dump-periods with warm-up or cool-down 0: the saving is inflated by the empty cluster at the window edges")
    cidservice = CarbonIntensityService(head_fraction=head_fraction, head_weight=head_weight, max_wait_hours=max_wait)

    print(" ".join(sys.argv))

    if jobs_file:
        get_jobs, get_job_batches = get_jobs_from_parquet, get_job_batches_from_parquet
    else:
        get_jobs, get_job_batches = get_jobs_from_db, get_job_batches_from_db

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
        waits = []
        jobs, min_start, max_end = get_jobs_and_energy(start_time, end_time, by_submit_time, get_jobs, jobs_file, waits)
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
        jobs2, min_start2, max_end2, horizon_stats = get_jobs_and_energy_reschedule(start_time, end_time, by_submit_time, get_job_batches, jobs_file, cidservice, strict=strict, reschedule_fraction=reschedule_percent / 100, reschedule_seed=reschedule_seed, defer=defer, retry_timeout=retry_timeout, count_violations=count_violations, warmup_days=warmup_days, cooldown_days=cooldown_days)
        periods2 = get_graph_data(jobs2, min_start2, max_end2, cidservice)
        if dump_periods:
            dump_periods_json(dump_periods, cidservice, chosen_day, days_to_fetch, periods, periods2, horizon_stats)
        total_energy = sum(period["energy"] for period in periods)
        if horizon_stats["unschedulable_jobs"]:
            energy_frac = 100 * horizon_stats["unschedulable_energy"] / total_energy
            logger.info(
                f"{horizon_stats['unschedulable_jobs']} job(s) beyond the carbon forecast horizon "
                f"(or otherwise unplaceable) were left at their original start ({energy_frac:.2f}% of energy)"
            )
        if horizon_stats["crowded_out_jobs"]:
            energy_frac = 100 * horizon_stats["crowded_out_energy"] / total_energy
            logger.info(
                f"{horizon_stats['crowded_out_jobs']} job(s) found no window with free capacity and were "
                f"left at their original start ({energy_frac:.2f}% of energy)"
            )
        if horizon_stats.get("never_placed_jobs"):
            energy_frac = 100 * horizon_stats["never_placed_energy"] / total_energy
            logger.info(
                f"{horizon_stats['never_placed_jobs']} deferred job(s) found no window within {retry_timeout:g}h and were "
                f"left at their original start ({energy_frac:.2f}% of energy)"
            )
        if "violating_pin_jobs" in horizon_stats:
            logger.info(
                f"{horizon_stats['violating_pin_jobs']} pinned job(s) had no node with room at their original window "
                f"({100 * horizon_stats['violating_pin_energy'] / total_energy:.2f}% of energy); "
                f"{horizon_stats['unverifiable_pin_jobs']} could not be checked"
            )
        result_json |= horizon_stats | summarise_waits(waits)
        if warmup_days:
            result_json["warmup_days"] = warmup_days
        if cooldown_days:
            result_json["cooldown_days"] = cooldown_days
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