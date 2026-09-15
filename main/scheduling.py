from collections import defaultdict
from datetime import UTC, datetime, timedelta
from functools import cmp_to_key

from fetchcarbon import FORECAST_HORIZON
from tqdm import tqdm


class VirtualCapacityExhausted(Exception):
    """No window in the carbon forecast has enough free nodes to host the job."""


class Node:
    def __init__(self):
        # TODO: Could set these arbitrarily ridiculously high, to show imaginary lowest possible
        self.NUM_CPUS = 2
        self.NUM_CORES_PER_CPU = 96
        self.MEM_GIB = 1536

        self.jobs = defaultdict(list)
        self.cap_cores = self.NUM_CPUS * self.NUM_CORES_PER_CPU
        self.cap_mem = self.MEM_GIB * 1024

    def can_assign(self, job, time_start, time_end=None, now=None):
        if time_end is None:
            time_end = time_start + timedelta(minutes=job["timelimit"])

        assigned_cores, assigned_mem = 0, 0
        for (ajob_start, ajob_end), ajobs in self.jobs.items():
            for ajob in ajobs:
                actual_end = ajob_start + timedelta(seconds=ajob["time"]["elapsed"])
                if now is not None and now >= actual_end:
                    effective_end = min(actual_end, ajob_end)
                else:
                    effective_end = ajob_end

                if time_start < effective_end and ajob_start < time_end:
                    assigned_cores += ajob["req"]["cpu"]
                    assigned_mem += ajob["req"]["mem"]
                    if (assigned_cores + job["req"]["cpu"] > self.cap_cores) or (assigned_mem + job["req"]["mem"] > self.cap_mem):
                        return False
        return True
    
    def assign(self, job, time_start, time_end=None, skip_check=False):
        if time_end is None:
            time_end = time_start + timedelta(minutes=job["timelimit"])
        if skip_check or self.can_assign(job, time_start, time_end):
            self.jobs[(time_start, time_end)].append(job)
            return True
        return False

def job_footprint(job):
    # TODO: Currently hardcoded to JASMIN's identical nodes
    cpu_frac = job["req"]["cpu"] / (2 * 96)
    mem_frac = job["req"]["mem"] / (1536 * 1024)
    return job["timelimit"] * max(cpu_frac, mem_frac)

def job_comparator(a, b):
    return job_footprint(b) - job_footprint(a)

class CarbonScheduler:
    def __init__(self, cidservice):
        self.NUM_NODES = 255
        self.nodes = [Node() for node in range(self.NUM_NODES)]
        self.cidservice = cidservice

        self.unschedulable = []

    def _pin_unschedulable(self, job):
        self.unschedulable.append(job)
        anchor = job["time"]["start"] or job["time"]["submit"]
        pinned_start = datetime.fromtimestamp(anchor, tz=UTC)
        # clamp so Slurm's INFINITE timelimit can't overflow datetime.max
        duration = min(timedelta(minutes=job["timelimit"]), datetime.max.replace(tzinfo=UTC) - pinned_start)
        pinned_end = pinned_start + duration
        for node in self.nodes[: job["req"]["node"]]:
            node.assign(job, pinned_start, pinned_end, skip_check=True)

    def process_jobs(self, jobs):
        jobs = sorted(jobs, key=cmp_to_key(job_comparator))
        for job in tqdm(jobs, leave=False, disable=None):
            num_nodes_req = job["req"]["node"]

            # avoids overflow in yield_lowest_periods for unbounded/huge timelimits
            if timedelta(minutes=job["timelimit"]) > FORECAST_HORIZON:
                self._pin_unschedulable(job)
                continue

            submit_time = datetime.fromtimestamp(job["time"]["submit"], tz=UTC)
            windows = self.cidservice.yield_lowest_periods(submit_time, job["timelimit"] * 60)

            if not windows:
                self._pin_unschedulable(job)
                continue

            best = None
            for _, window_start, window_end in windows:
                chosen = []
                for node in self.nodes:
                    if node.can_assign(job, window_start, window_end, now=submit_time):
                        chosen.append(node)
                        if len(chosen) == num_nodes_req:
                            break
                if len(chosen) == num_nodes_req:
                    best = (chosen, window_start, window_end)
                    break

            if best is None:
                raise VirtualCapacityExhausted(
                    f"No window with {num_nodes_req} available nodes for job {job['id_job']}"
                )

            chosen, window_start, window_end = best
            for node in chosen:
                node.assign(job, window_start, window_end, skip_check=True)

    def output_jobs(self):
        seen = set()
        for node in self.nodes:
            for (time_start, time_end), jobs in node.jobs.items():
                for job in jobs:
                    if id(job) in seen:
                        continue
                    seen.add(id(job))
                    job["time"]["start"] = int(time_start.timestamp())
                    job["time"]["end"] = job["time"]["start"] + job["time"]["elapsed"]
                    yield job