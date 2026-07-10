from datetime import datetime, timedelta
from fetchcarbon import yield_lowest_periods
from collections import defaultdict
from tqdm import tqdm

class Node:
    def __init__(self):
        # TODO: Could set these arbitrarily ridiculously high, to show imaginary lowest possible
        self.NUM_CPUS = 2
        self.NUM_CORES_PER_CPU = 96
        self.MEM_GIB = 1536
        self.jobs = defaultdict(list)

    def can_assign(self, job, time_start, time_end=None):
        if time_end is None:
            time_end = time_start + timedelta(minutes=job["timelimit"])

        cap_cores = self.NUM_CPUS * self.NUM_CORES_PER_CPU
        cap_mem = self.MEM_GIB * 1024

        assigned_cores, assigned_mem = 0, 0
        for (ajob_start, ajob_end), ajobs in self.jobs.items():
            if time_start < ajob_end and ajob_start < time_end:
                for ajob in ajobs:
                    assigned_cores += ajob["req"]["cpu"]
                    assigned_mem += ajob["req"]["mem"]
                    if (assigned_cores + job["req"]["cpu"] > cap_cores) or (assigned_mem + job["req"]["mem"] > cap_mem):
                        return False
        return True
    
    def assign(self, job, time_start, time_end=None):
        if time_end is None:
            time_end = time_start + timedelta(minutes=job["timelimit"])
        if self.can_assign(job, time_start, time_end):
            self.jobs[(time_start, time_end)].append(job)
            return True
        return False

class CarbonScheduler:
    def __init__(self):
        self.NUM_NODES = 255
        self.nodes = [Node() for node in range(self.NUM_NODES)]
    
    def process_jobs(self, jobs):
        for job in tqdm(jobs, leave=False):
            assert job["req"]["node"] == 1
            submit_time = datetime.fromtimestamp(job["time"]["submit"])
            node_index = 0
            while True:
                assigned = False
                for value, window_start, window_end in yield_lowest_periods(submit_time, job["timelimit"]):
                    # print(value, window_start, window_end)
                    if self.nodes[node_index].assign(job, window_start, window_end):
                        assigned = True
                        break
                if assigned:
                    break
                else:
                    node_index += 1
    
    def output_jobs(self):
        for node in self.nodes:
            for (time_start, time_end), jobs in node.jobs.items():
                for job in jobs:
                    job["time"]["start"] = int(time_start.timestamp())
                    job["time"]["end"] = job["time"]["start"] + job["time"]["elapsed"]
                    yield job