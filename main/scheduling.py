from collections import defaultdict
from datetime import UTC, datetime, timedelta
from functools import cmp_to_key

from fetchcarbon import FORECAST_HORIZON
from tqdm import tqdm

PIN_IF_FEWER_WINDOWS_THAN = 2


class VirtualCapacityExhausted(Exception):
    """No window has enough free nodes. Only raised under strict=True."""


def _effective_end(job, time_start, time_end):
    reservation_s = (time_end - time_start).total_seconds()
    elapsed = job["time"]["elapsed"]
    if elapsed < reservation_s:
        return time_start + timedelta(seconds=elapsed)
    return time_end


class Node:
    def __init__(self):
        # TODO: Could set these arbitrarily ridiculously high, to show imaginary lowest possible
        self.NUM_CPUS = 2
        self.NUM_CORES_PER_CPU = 96
        self.MEM_GIB = 1536

        self.jobs = defaultdict(list)
        self.cap_cores = self.NUM_CPUS * self.NUM_CORES_PER_CPU
        self.cap_mem = self.MEM_GIB * 1024

        self._bucket_totals = {}
        self._peak_cache = {}
        self._bucket_sum_cache = {}
        self._peak_floor = {}  # per-window lower bound on peak usage; never cleared, reject-only

    def _bucket_sum(self, time_start, time_end):
        cached = self._bucket_sum_cache.get((time_start, time_end))
        if cached is not None:
            return cached
        summed_cores, summed_mem = 0, 0
        for (ajob_start, ajob_end), (cpu_total, mem_total) in self._bucket_totals.items():
            if time_start < ajob_end and ajob_start < time_end:
                summed_cores += cpu_total
                summed_mem += mem_total
        self._bucket_sum_cache[(time_start, time_end)] = (summed_cores, summed_mem)
        return summed_cores, summed_mem

    def _peak_usage(self, time_start, time_end):
        cached = self._peak_cache.get((time_start, time_end))
        if cached is not None:
            return cached

        events = []
        for (ajob_start, ajob_end), ajobs in self.jobs.items():
            if not (time_start < ajob_end and ajob_start < time_end):
                continue
            for ajob, effective_end in ajobs:
                if effective_end <= time_start:
                    continue
                ocpu = ajob["req"]["cpu"] // ajob["req"]["node"]
                omem = ajob["req"]["mem"] // ajob["req"]["node"]
                events.append((max(ajob_start, time_start), 1, ocpu, omem))
                events.append((min(effective_end, time_end), 0, -ocpu, -omem))
        events.sort()  # releases (tag 0) before acquires (tag 1) at an equal timestamp

        peak_cores, peak_mem = 0, 0
        cur_cores, cur_mem = 0, 0
        for _, _tag, dcpu, dmem in events:
            cur_cores += dcpu
            cur_mem += dmem
            peak_cores = max(peak_cores, cur_cores)
            peak_mem = max(peak_mem, cur_mem)

        self._peak_cache[(time_start, time_end)] = (peak_cores, peak_mem)

        key = (time_start, time_end)
        prev_floor = self._peak_floor.get(key)
        self._peak_floor[key] = (
            (peak_cores, peak_mem) if prev_floor is None
            else (max(prev_floor[0], peak_cores), max(prev_floor[1], peak_mem))
        )

        return peak_cores, peak_mem

    def can_assign(self, job, time_start, time_end=None):
        if time_end is None:
            time_end = time_start + timedelta(minutes=job["timelimit"])

        # req.cpu/req.mem are totals across all req.node nodes -- divide to get one node's share.
        req_cpu, req_mem = job["req"]["cpu"] // job["req"]["node"], job["req"]["mem"] // job["req"]["node"]

        floor = self._peak_floor.get((time_start, time_end))
        if floor is not None and (floor[0] + req_cpu > self.cap_cores or floor[1] + req_mem > self.cap_mem):
            return False

        summed_cores, summed_mem = self._bucket_sum(time_start, time_end)
        if summed_cores + req_cpu <= self.cap_cores and summed_mem + req_mem <= self.cap_mem:
            return True

        peak_cores, peak_mem = self._peak_usage(time_start, time_end)
        return peak_cores + req_cpu <= self.cap_cores and peak_mem + req_mem <= self.cap_mem

    def assign(self, job, time_start, time_end=None, skip_check=False):
        if time_end is None:
            time_end = time_start + timedelta(minutes=job["timelimit"])
        if skip_check or self.can_assign(job, time_start, time_end):
            effective_end = _effective_end(job, time_start, time_end)
            self.jobs[(time_start, time_end)].append((job, effective_end))

            req_cpu, req_mem = job["req"]["cpu"] // job["req"]["node"], job["req"]["mem"] // job["req"]["node"]
            key = (time_start, effective_end)
            cpu_total, mem_total = self._bucket_totals.get(key, (0, 0))
            self._bucket_totals[key] = (cpu_total + req_cpu, mem_total + req_mem)
            self._peak_cache.clear()
            self._bucket_sum_cache.clear()
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
    def __init__(self, cidservice, strict=False):
        self.NUM_NODES = 255
        self.nodes = [Node() for node in range(self.NUM_NODES)]
        self.cidservice = cidservice
        self.strict = strict  # raise VirtualCapacityExhausted instead of pinning crowded-out jobs

        self.unschedulable = []
        self.crowded_out = []
        self.placements = []  # (job, start_dt)
        self._node_cursor = {}  # (window_start, window_end, shape) -> first node not yet proven full

    def _pin_unschedulable(self, job, crowded_out=False):
        (self.crowded_out if crowded_out else self.unschedulable).append(job)
        anchor = job["time"]["start"] or job["time"]["submit"]
        pinned_start = datetime.fromtimestamp(anchor, tz=UTC)
        # clamp so Slurm's INFINITE timelimit can't overflow datetime.max
        duration = min(timedelta(minutes=job["timelimit"]), datetime.max.replace(tzinfo=UTC) - pinned_start)
        pinned_end = pinned_start + duration
        for node in self.nodes[: job["req"]["node"]]:
            node.assign(job, pinned_start, pinned_end, skip_check=True)
        self.placements.append((job, pinned_start))

    def _is_permanently_infeasible(self, job):
        # can never fit any window
        num_nodes = job["req"]["node"]
        if num_nodes > self.NUM_NODES:
            return True
        per_node_cpu = job["req"]["cpu"] // num_nodes
        per_node_mem = job["req"]["mem"] // num_nodes
        return per_node_cpu > self.nodes[0].cap_cores or per_node_mem > self.nodes[0].cap_mem

    def process_jobs(self, jobs):
        jobs = sorted(jobs, key=cmp_to_key(job_comparator))
        for job in tqdm(jobs, leave=False, disable=None):
            num_nodes_req = job["req"]["node"]

            if self._is_permanently_infeasible(job):
                self._pin_unschedulable(job)
                continue

            # avoids overflow in yield_lowest_periods for unbounded/huge timelimits
            if timedelta(minutes=job["timelimit"]) > FORECAST_HORIZON:
                self._pin_unschedulable(job)
                continue

            submit_time = datetime.fromtimestamp(job["time"]["submit"], tz=UTC)
            windows = self.cidservice.yield_lowest_periods(submit_time, job["timelimit"] * 60)

            if not windows:
                self._pin_unschedulable(job)
                continue

            shape = (job["req"]["cpu"] // num_nodes_req, job["req"]["mem"] // num_nodes_req)
            best = None
            for _, window_start, window_end in windows:
                cursor_key = (window_start, window_end, shape)
                start_index = self._node_cursor.get(cursor_key, 0)
                if start_index >= self.NUM_NODES:
                    continue  # every node already proven full for this shape+window

                chosen = []
                advancing = True  # still extending the known-full prefix
                for index in range(start_index, self.NUM_NODES):
                    if self.nodes[index].can_assign(job, window_start, window_end):
                        chosen.append(self.nodes[index])
                        if advancing:
                            self._node_cursor[cursor_key] = index  # this node isn't full; stop here
                            advancing = False
                        if len(chosen) == num_nodes_req:
                            break
                    elif advancing:
                        self._node_cursor[cursor_key] = index + 1

                if len(chosen) == num_nodes_req:
                    best = (chosen, window_start, window_end)
                    break

            if best is None:
                if len(windows) < PIN_IF_FEWER_WINDOWS_THAN:
                    self._pin_unschedulable(job)
                    continue
                if self.strict:
                    raise VirtualCapacityExhausted(
                        f"No window with {num_nodes_req} available nodes for job {job['id_job']}"
                    )
                self._pin_unschedulable(job, crowded_out=True)
                continue

            chosen, window_start, window_end = best
            for node in chosen:
                node.assign(job, window_start, window_end, skip_check=True)
            self.placements.append((job, window_start))

    def output_jobs(self):
        for job, start_dt in self.placements:
            job["time"]["start"] = int(start_dt.timestamp())
            job["time"]["end"] = job["time"]["start"] + job["time"]["elapsed"]
            yield job