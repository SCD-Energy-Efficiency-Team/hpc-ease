import hashlib
from bisect import bisect_left, bisect_right
from datetime import UTC, datetime, timedelta
from functools import cmp_to_key

from fetchcarbon import FORECAST_HORIZON, round_to_30min
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

        self.cap_cores = self.NUM_CPUS * self.NUM_CORES_PER_CPU
        self.cap_mem = self.MEM_GIB * 1024

        # usage skyline: on [_sky_t[i], _sky_t[i+1]) the node holds _sky_cores[i] cores and _sky_mem[i] MiB
        self._sky_t = []
        self._sky_cores = []
        self._sky_mem = []

    def _split(self, t):
        # make t a breakpoint (copying the usage it falls inside) and return its index
        times = self._sky_t
        i = bisect_left(times, t)
        if i == len(times) or times[i] != t:
            times.insert(i, t)
            self._sky_cores.insert(i, self._sky_cores[i - 1] if i else 0)
            self._sky_mem.insert(i, self._sky_mem[i - 1] if i else 0)
        return i

    def _peak_usage(self, start, end):
        # peak concurrent (cores, mem) over [start, end), epoch seconds
        times = self._sky_t
        first = max(bisect_right(times, start) - 1, 0)
        last = bisect_left(times, end)
        if last <= first:
            return 0, 0
        return max(self._sky_cores[first:last]), max(self._sky_mem[first:last])

    def _fits(self, req_cpu, req_mem, start, end):
        peak_cores, peak_mem = self._peak_usage(start, end)
        return peak_cores + req_cpu <= self.cap_cores and peak_mem + req_mem <= self.cap_mem

    def can_assign(self, job, time_start, time_end=None):
        if time_end is None:
            time_end = time_start + timedelta(minutes=job["timelimit"])

        # req.cpu/req.mem are totals across all req.node nodes -- divide to get one node's share.
        req_cpu, req_mem = job["req"]["cpu"] // job["req"]["node"], job["req"]["mem"] // job["req"]["node"]
        return self._fits(req_cpu, req_mem, int(time_start.timestamp()), int(time_end.timestamp()))

    def assign(self, job, time_start, time_end=None, skip_check=False):
        if time_end is None:
            time_end = time_start + timedelta(minutes=job["timelimit"])
        if skip_check or self.can_assign(job, time_start, time_end):
            start = int(time_start.timestamp())
            end = int(_effective_end(job, time_start, time_end).timestamp())
            if end > start:
                req_cpu, req_mem = job["req"]["cpu"] // job["req"]["node"], job["req"]["mem"] // job["req"]["node"]
                first, last = self._split(start), self._split(end)
                for i in range(first, last):
                    self._sky_cores[i] += req_cpu
                    self._sky_mem[i] += req_mem
            return True
        return False

    def retire_before(self, watermark):
        # keep the segment holding the watermark; nothing before it is ever queried again
        keep_from = bisect_right(self._sky_t, int(watermark.timestamp())) - 1
        if keep_from > 0:
            del self._sky_t[:keep_from], self._sky_cores[:keep_from], self._sky_mem[:keep_from]


def job_footprint(job):
    # TODO: Currently hardcoded to JASMIN's identical nodes
    cpu_frac = job["req"]["cpu"] / (2 * 96)
    mem_frac = job["req"]["mem"] / (1536 * 1024)
    return job["timelimit"] * max(cpu_frac, mem_frac)


def job_comparator(a, b):
    return job_footprint(b) - job_footprint(a)


class CarbonScheduler:
    def __init__(self, cidservice, strict=False, energy_fn=None, reschedule_fraction=1.0, reschedule_seed=0,
                 defer=False, retry_timeout=timedelta(hours=24), count_violations=False,
                 measure_from=None, measure_until=None):
        self.NUM_NODES = 255
        self.nodes = [Node() for node in range(self.NUM_NODES)]
        self.cidservice = cidservice
        self.strict = strict  # raise VirtualCapacityExhausted instead of pinning crowded-out jobs
        self._watermark = None
        self._retire_due_at = [0] * self.NUM_NODES

        self.reschedule_seed = reschedule_seed
        self.reschedule_fraction = reschedule_fraction  # share of jobs the scheduler may move; the rest keep their original start
        self.unschedulable = []
        self.crowded_out = []
        self.left_alone = []
        self.defer = defer  # queue crowded-out jobs and retry them as the forecast horizon advances
        self.retry_timeout = retry_timeout  # max time a deferred job stays unplaced (now - submit)
        self.check_pins = count_violations
        self.violating_pins = []  # pinned jobs that no set of nodes could take at their original window
        self.unverifiable_pins = []  # pinned jobs whose window starts before the retirement watermark, so occupancy there is gone
        self.pending = []  # (job, horizon_end, original_score): latest window end already tried, and the score of the job's original window
        self.deferred = []  # every job that was ever queued
        self.never_placed = []  # deferred jobs pinned at their original start after retry_timeout
        self._last_retry_grid = None
        self._last_tick = None
        self.measure_from = measure_from  # epoch: jobs submitted outside [measure_from, measure_until) take capacity but are never reported
        self.measure_until = measure_until
        self.energy_fn = energy_fn  # if set, placements keep (energy, start_dt, elapsed, submit) instead of the job dict
        self.placements = []  # (job, start_dt), or (energy, start_dt, elapsed, submit) when energy_fn is set
        self.placed_waits = []  # start - submit of measured jobs the algorithm placed in a window (not pinned ones)
        self._node_cursor = {}  # (window_start, window_end, shape) -> first node not yet proven full

    def _maybe_retire(self):
        if self._watermark is None:
            return
        for index, node in enumerate(self.nodes):
            if len(node._sky_t) >= self._retire_due_at[index]:
                node.retire_before(self._watermark)
                self._retire_due_at[index] = max(len(node._sky_t) * 2, 16)
        stale_keys = [k for k in self._node_cursor if k[0] < self._watermark]
        for key in stale_keys:
            del self._node_cursor[key]

    def _tag_unmeasured(self, job):
        submit = job["time"]["submit"]
        before = self.measure_from is not None and submit < self.measure_from
        after = self.measure_until is not None and submit >= self.measure_until
        job["unmeasured"] = before or after

    @staticmethod
    def _measured(job):
        return not job.get("unmeasured")

    def _record_placement(self, job, start_dt):
        if not self._measured(job):
            return
        if self.energy_fn is None:
            self.placements.append((job, start_dt))
        else:
            self.placements.append((self.energy_fn(job), start_dt, job["time"]["elapsed"], job["time"]["submit"]))

    def _is_selected(self, job):
        # Bernoulli draw from a keyed hash: same job, same answer in any window or batch order
        if self.reschedule_fraction >= 1.0:
            return True
        digest = hashlib.blake2b(f"{self.reschedule_seed}:{job['job_db_inx']}".encode(), digest_size=8).digest()
        return int.from_bytes(digest, "big") / 2**64 < self.reschedule_fraction

    def _pin_unschedulable(self, job, crowded_out=False, left_alone=False):
        if self._measured(job):
            (self.left_alone if left_alone else self.crowded_out if crowded_out else self.unschedulable).append(job)
        self._pin(job, check=not left_alone)  # a left-alone job stays put by design, so it is never a violation

    def _pin_never_placed(self, job):
        if self._measured(job):
            self.never_placed.append(job)
        self._pin(job)

    def _pin_window(self, job):
        anchor = job["time"]["start"] or job["time"]["submit"]
        pinned_start = datetime.fromtimestamp(anchor, tz=UTC)
        # clamp so Slurm's INFINITE timelimit can't overflow datetime.max
        duration = min(timedelta(minutes=job["timelimit"]), datetime.max.replace(tzinfo=UTC) - pinned_start)
        return pinned_start, pinned_start + duration

    def _hold_back_for_pending(self, watermark):
        # when checking pins, keep every pending job's original window intact: retirement only drops reservations
        # ending at or before the watermark, so a watermark at the earliest pending original start loses nothing
        if not (self.check_pins and self.pending):
            return watermark
        earliest = min(self._pin_window(job)[0] for job, _, _ in self.pending)
        return min(watermark, datetime.fromtimestamp(round_to_30min(earliest.timestamp(), up=False, iso=False), tz=UTC))

    def _pin(self, job, check=True):
        pinned_start, pinned_end = self._pin_window(job)
        target_nodes = self.nodes[: job["req"]["node"]]
        if self.check_pins and check:
            if self._watermark is not None and pinned_start < self._watermark:
                if self._measured(job):
                    self.unverifiable_pins.append(job)  # retired state would make every node look free
            else:
                # checked before assigning, so the job's own occupancy isn't counted against itself
                if self._find_placement(job, [(0, pinned_start, pinned_end)]) is None and self._measured(job):
                    self.violating_pins.append(job)
        for node in target_nodes:
            node.assign(job, pinned_start, pinned_end, skip_check=True)
        self._record_placement(job, pinned_start)

    def _is_permanently_infeasible(self, job):
        # can never fit any window
        num_nodes = job["req"]["node"]
        if num_nodes > self.NUM_NODES:
            return True
        per_node_cpu = job["req"]["cpu"] // num_nodes
        per_node_mem = job["req"]["mem"] // num_nodes
        return per_node_cpu > self.nodes[0].cap_cores or per_node_mem > self.nodes[0].cap_mem

    def _find_placement(self, job, windows):
        num_nodes_req = job["req"]["node"]
        shape = (job["req"]["cpu"] // num_nodes_req, job["req"]["mem"] // num_nodes_req)
        for _, window_start, window_end in windows:
            cursor_key = (window_start, window_end, shape)
            start_index = self._node_cursor.get(cursor_key, 0)
            if start_index >= self.NUM_NODES:
                continue  # every node already proven full for this shape+window

            chosen = []
            advancing = True  # still extending the known-full prefix
            start_s, end_s = int(window_start.timestamp()), int(window_end.timestamp())
            for index in range(start_index, self.NUM_NODES):
                if self.nodes[index]._fits(shape[0], shape[1], start_s, end_s):
                    chosen.append(self.nodes[index])
                    if advancing:
                        self._node_cursor[cursor_key] = index  # this node isn't full; stop here
                        advancing = False
                    if len(chosen) == num_nodes_req:
                        break
                elif advancing:
                    self._node_cursor[cursor_key] = index + 1

            if len(chosen) == num_nodes_req:
                return chosen, window_start, window_end
        return None

    def _place(self, best, job):
        chosen, window_start, window_end = best
        for node in chosen:
            node.assign(job, window_start, window_end, skip_check=True)
        self._record_placement(job, window_start)
        if self._measured(job):
            self.placed_waits.append(int(window_start.timestamp()) - job["time"]["submit"])

    def _original_score(self, job):
        # weighted score of the window the job would get if pinned at its original start
        anchor = datetime.fromtimestamp(job["time"]["start"] or job["time"]["submit"], tz=UTC)
        return self.cidservice.score_window(anchor, job["timelimit"] * 60)

    def _retry_pending(self, now):
        max_wait = getattr(self.cidservice, "max_wait", None)
        still_pending = []
        footprint_order = cmp_to_key(job_comparator)
        for job, horizon_end, original_score in sorted(self.pending, key=lambda item: footprint_order(item[0])):
            submit_time = datetime.fromtimestamp(job["time"]["submit"], tz=UTC)
            last_start = None
            if max_wait is not None:
                last_start = datetime.fromtimestamp(round_to_30min(job["time"]["submit"], False, iso=False), tz=UTC) + max_wait
            if now - submit_time > self.retry_timeout or (last_start is not None and now > last_start):
                self._pin_never_placed(job)
                continue

            # occupancy only grows, so windows already tried stay full: only windows ending past horizon_end are new
            windows = self.cidservice.yield_lowest_periods(now, job["timelimit"] * 60)
            edge = [w for w in windows if w[2] > horizon_end and w[0] < original_score and w[1] >= now
                    and (last_start is None or w[1] <= last_start)]
            if windows:
                horizon_end = max(horizon_end, max(w[2] for w in windows))
            best = self._find_placement(job, edge) if edge else None
            if best is None:
                still_pending.append((job, horizon_end, original_score))
            else:
                self._place(best, job)
        self.pending = still_pending

    def finalize(self):
        """Keep stepping the clock until every deferred job is placed or has timed out."""
        if self._last_tick is None:
            for job, _, _ in self.pending:
                self._pin_never_placed(job)
            self.pending = []
            return
        tick = self._last_tick
        while self.pending:
            tick += 1800
            self._retry_pending(datetime.fromtimestamp(tick, tz=UTC))

    def process_jobs(self, jobs, batch_start_epoch=None):
        if self.measure_from is not None or self.measure_until is not None:
            for job in jobs:
                self._tag_unmeasured(job)

        if self.defer and batch_start_epoch is not None:
            grid = round_to_30min(batch_start_epoch, False, iso=False)
            if self.pending and self._last_retry_grid is not None and grid > self._last_retry_grid:
                self._retry_pending(datetime.fromtimestamp(grid, tz=UTC))
            self._last_retry_grid = grid
            self._last_tick = grid

        if batch_start_epoch is not None:  # streaming retirement; omit the epoch to keep all state
            now_epoch = datetime.now(tz=UTC).timestamp()
            floored = round_to_30min(min(batch_start_epoch, now_epoch), up=False, iso=False)
            new_watermark = datetime.fromtimestamp(floored, tz=UTC)
            new_watermark = self._hold_back_for_pending(new_watermark)
            self._watermark = new_watermark if self._watermark is None else max(self._watermark, new_watermark)
            self._maybe_retire()

        if self.reschedule_fraction < 1.0:
            # unselected jobs go in first so the selected ones are placed around them
            moved = []
            for job in jobs:
                if self._is_selected(job):
                    moved.append(job)
                else:
                    self._pin_unschedulable(job, left_alone=True)
            jobs = moved

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
            # the lookahead starts at floor(submit); drop windows that begin before the job existed
            windows = [w for w in windows if w[1] >= submit_time]

            if not windows:
                self._pin_unschedulable(job)
                continue

            best = self._find_placement(job, windows)

            if best is None:
                if len(windows) < PIN_IF_FEWER_WINDOWS_THAN:
                    self._pin_unschedulable(job)
                    continue
                if self.strict:
                    raise VirtualCapacityExhausted(
                        f"No window with {num_nodes_req} available nodes for job {job['id_job']}"
                    )
                if self.defer:
                    self.pending.append((job, max(w[2] for w in windows), self._original_score(job)))
                    if self._measured(job):
                        self.deferred.append(job)
                    continue
                self._pin_unschedulable(job, crowded_out=True)
                continue

            self._place(best, job)

    def output_energies(self):
        for energy, start_dt, elapsed, _ in self.placements:
            start = int(start_dt.timestamp())
            yield start, start + elapsed, energy

    def output_waits(self):
        for _, start_dt, _, submit in self.placements:
            yield int(start_dt.timestamp()) - submit

    def output_placed_waits(self):
        return iter(self.placed_waits)

    def output_jobs(self):
        for job, start_dt in self.placements:
            job["time"]["start"] = int(start_dt.timestamp())
            job["time"]["end"] = job["time"]["start"] + job["time"]["elapsed"]
            yield job