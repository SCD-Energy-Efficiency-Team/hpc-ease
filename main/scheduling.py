from collections import defaultdict
from datetime import UTC, datetime, timedelta
from functools import cmp_to_key


class VirtualCapacityExhausted(Exception):
    pass


class Node:
    def __init__(self):
        # TODO: Could set these arbitrarily ridiculously high, to show imaginary lowest possible
        self.NUM_CPUS = 2
        self.NUM_CORES_PER_CPU = 96
        self.MEM_GIB = 1536

        self.jobs = defaultdict(list)
        self.cap_cores = self.NUM_CPUS * self.NUM_CORES_PER_CPU
        self.cap_mem = self.MEM_GIB * 1024

    def can_assign(self, job, time_start, time_end=None):
        if time_end is None:
            time_end = time_start + timedelta(minutes=job["timelimit"])

        assigned_cores, assigned_mem = 0, 0
        for (ajob_start, ajob_end), ajobs in self.jobs.items():
            if time_start < ajob_end and ajob_start < time_end:
                for ajob in ajobs:
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
        self.jobs_seen = 0

    def process_jobs(self, jobs):
        jobs = sorted(jobs, key=cmp_to_key(job_comparator))
        for job in jobs:
            self.jobs_seen += 1
            num_nodes_req = job["req"]["node"]
            submit_time = datetime.fromtimestamp(job["time"]["submit"], tz=UTC)
            windows = self.cidservice.yield_lowest_periods(submit_time, job["timelimit"])
            available = []
            for node_index, node in enumerate(self.nodes):
                for _, window_start, window_end in windows:
                    if node.can_assign(job, window_start, window_end):
                        available.append(node_index)
                        break
                if len(available) == num_nodes_req:
                    break
            if len(available) != num_nodes_req:
                raise VirtualCapacityExhausted(
                    f"Only {len(available)} of {num_nodes_req} nodes available for job {job['id_job']}"
                )
            for node_index in available:
                node.assign(job, window_start, window_end, skip_check=True)
    
    def output_jobs(self):
        for node in self.nodes:
            for (time_start, time_end), jobs in node.jobs.items():
                for job in jobs:
                    job["time"]["start"] = int(time_start.timestamp())
                    job["time"]["end"] = job["time"]["start"] + job["time"]["elapsed"]
                    yield job