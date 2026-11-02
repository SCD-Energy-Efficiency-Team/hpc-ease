import bisect
import csv
import logging
import math
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from functools import lru_cache

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DEFAULT_HEAD_FRACTION = 0.25
DEFAULT_HEAD_WEIGHT = 4.0  # 1.0 = flat score (unchanged behaviour)

YIELD_CACHE_SIZE = 4096
CLIMATE_CACHE_SIZE = 512

def gen_iso_string(epoch_time):
    return datetime.fromtimestamp(epoch_time, tz=UTC).strftime("%Y-%m-%dT%H:%MZ")

def round_to_30min(epoch_time, up, iso=True):
    if up:
        epoch_time = math.ceil(epoch_time / 1800) * 1800
    else:
        epoch_time = (epoch_time // 1800) * 1800
    if iso:
        return gen_iso_string(epoch_time)
    else:
        return epoch_time

def get_rounded_iso_range(start_epoch, end_epoch):
    return round_to_30min(start_epoch, False), round_to_30min(end_epoch, True)

class MissingCarbonData(Exception):
    pass

def build_intensity_prefix(intensity_data):
    prefix = []

    for (start, end), intensity in sorted(intensity_data.items()):
        prefix.append((start, end, intensity))

    return prefix

def build_intensity_cumsum(prefix):
    starts = []
    cum_intensity = [0.0]
    cum_covered = [timedelta(0)]
    for period_start, period_end, intensity in prefix:
        starts.append(period_start)
        span = period_end - period_start
        cum_intensity.append(cum_intensity[-1] + intensity * span.total_seconds() / 3600)
        cum_covered.append(cum_covered[-1] + span)
    return starts, cum_intensity, cum_covered

def calculate_cumulative_intensity(window_start, window_end, prefix, cumsum=None):
    if cumsum is None:
        cumsum = build_intensity_cumsum(prefix)
    starts, cum_intensity, cum_covered = cumsum

    if window_end <= window_start or not starts:
        return 0.0, timedelta(0)

    def integral_to(point):
        i = bisect.bisect_right(starts, point) - 1
        if i < 0:
            return 0.0, timedelta(0)
        period_start, period_end, intensity = prefix[i]
        partial = max(min(point, period_end) - period_start, timedelta(0))
        return (
            cum_intensity[i] + intensity * partial.total_seconds() / 3600,
            cum_covered[i] + partial,
        )

    end_intensity, end_covered = integral_to(window_end)
    start_intensity, start_covered = integral_to(window_start)
    return end_intensity - start_intensity, end_covered - start_covered

def calculate_weighted_intensity(window_start, window_end, prefix, cumsum, head_fraction, head_weight):
    # first head_fraction of the window (split snapped to the 30-min grid) counts head_weight times the rest
    intensity, covered = calculate_cumulative_intensity(window_start, window_end, prefix, cumsum)
    if head_weight == 1.0:
        return intensity, covered
    duration = (window_end - window_start).total_seconds()
    head_s = min(duration, round(head_fraction * duration / 1800) * 1800)
    if head_s <= 0:
        return intensity, covered
    split = window_start + timedelta(seconds=head_s)
    head, _ = calculate_cumulative_intensity(window_start, split, prefix, cumsum)
    return head * head_weight + (intensity - head), covered

class CarbonIntensity:
    head_fraction = DEFAULT_HEAD_FRACTION
    head_weight = DEFAULT_HEAD_WEIGHT
    max_wait = None  # timedelta: latest window start after the floored submit time; None = whole horizon
    # subclasses set self.forecast_horizon_hours (from dataset.toml's [carbon] section)

    def _get_climate_data(self, start, end, returnvalues="forecast"):
        # returns {(start_datetime, end_datetime): intensityint, ...}
        raise NotImplementedError

    def score_window(self, start_datetime, duration: int):
        # same weighted score yield_lowest_periods ranks by, for an arbitrary (not grid-aligned) window;
        # calls the uncached _get_climate_data so one-off keys don't evict the horizon queries
        start_epoch = start_datetime.timestamp()
        end_epoch = start_epoch + duration
        limitstart, limitend = get_rounded_iso_range(int(start_epoch), int(end_epoch))
        prefix = build_intensity_prefix(self._get_climate_data(limitstart, limitend, "forecast"))
        score, _ = calculate_weighted_intensity(
            start_datetime, start_datetime + timedelta(seconds=duration),
            prefix, build_intensity_cumsum(prefix), self.head_fraction, self.head_weight,
        )
        return score

    def yield_lowest_periods(self, start_datetime, duration: int):
        start_datetime = min(start_datetime, datetime.now(tz=UTC))
        epoch = start_datetime.timestamp()
        return self._yield_lowest_periods_cached(
            round_to_30min(epoch, False, iso=False), epoch % 1800 == 0, duration
        )

    def _yield_lowest_periods(self, floored_epoch, is_grid_aligned, duration: int):
        # returns a list, with each item being:
        # (window_intensityint, start_datetime, end_datetime)

        limitstart = gen_iso_string(floored_epoch)
        limitend = gen_iso_string(floored_epoch + int(self.forecast_horizon_hours * 3600) + (0 if is_grid_aligned else 1800))
        carbon_intensity = self.get_climate_data(limitstart, limitend, "forecast")
        carbon_prefix = build_intensity_prefix(carbon_intensity)
        carbon_cumsum = build_intensity_cumsum(carbon_prefix)
        window_start = datetime.fromisoformat(limitstart)
        window_end = window_start + timedelta(seconds=duration)
        last_start = None if self.max_wait is None else window_start + self.max_wait

        # TODO: This might break in the case duration is less than the interval between window start and end?
        windows = defaultdict(list)
        while window_end < datetime.fromisoformat(limitend) and (last_start is None or window_start <= last_start):
            intensity, covered = calculate_weighted_intensity(
                window_start, window_end, carbon_prefix, carbon_cumsum, self.head_fraction, self.head_weight
            )

            if covered != timedelta(seconds=duration):
                break

            windows[intensity].append(window_start)

            window_start += timedelta(minutes=30)
            window_end = window_start + timedelta(seconds=duration)

        results = []
        for value in sorted(windows):
            for window_start in windows[value]:
                results.append((value, window_start, window_start + timedelta(seconds=duration)))
        return results

class CarbonIntensityCSV(CarbonIntensity):
    def __init__(self, csv_path, column, forecast_horizon_hours, head_fraction=DEFAULT_HEAD_FRACTION, head_weight=DEFAULT_HEAD_WEIGHT, max_wait_hours=None):
        # fixed at construction: the lru_cache below would otherwise serve stale windows
        self.csv_path = csv_path
        self.column = column
        self.forecast_horizon_hours = forecast_horizon_hours
        self.head_fraction = head_fraction
        self.head_weight = head_weight
        self.max_wait = None if max_wait_hours is None else timedelta(hours=max_wait_hours)
        self._yield_lowest_periods_cached = lru_cache(maxsize=YIELD_CACHE_SIZE)(self._yield_lowest_periods)
        self.get_climate_data = lru_cache(maxsize=CLIMATE_CACHE_SIZE)(self._get_climate_data)

        self.csv_data = None
        self.cache_csv_data()

    def cache_csv_data(self):
        self.csv_data = {}
        with open(self.csv_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            if self.column not in (reader.fieldnames or []):
                raise ValueError(f"{self.csv_path} has no column {self.column!r}; columns: {', '.join(reader.fieldnames or [])}")
            for row in reader:
                start = datetime.fromisoformat(row["datetime"]).replace(tzinfo=UTC)
                if start.minute % 30 or start.second or start.microsecond:
                    raise ValueError(f"{self.csv_path}: slot {row['datetime']} is not on a 30-minute boundary; the scheduler needs 30-minute slots")
                end = start + timedelta(minutes=30)
                value = row[self.column]
                if not value:
                    continue
                self.csv_data[(start, end)] = float(value)

    def _get_climate_data(self, start, end, returnvalues="forecast"):
        # returns {(start_datetime, end_datetime): intensityint, ...}
        assert returnvalues == "forecast", "Index is currently not supported"
        limitstart, limitend = datetime.fromisoformat(start), datetime.fromisoformat(end)
        start = limitstart
        results = {}
        while True:
            end = start + timedelta(minutes=30)
            iso_end = gen_iso_string(end.timestamp())
            key = (start, end)
            if key not in self.csv_data:
                raise MissingCarbonData(f"Missing data for {start} to {end}")
            results[key] = self.csv_data[key]
            if gen_iso_string(limitend.timestamp()) == iso_end:
                break
            start += timedelta(minutes=30)
        return results

if __name__ == "__main__":
    import random
    random.seed(42)
    logging.basicConfig(level=logging.INFO)
    from datasetconfig import load_dataset_config
    config = load_dataset_config(check_carbon=True)
    cidservice = CarbonIntensityCSV(config.carbon_csv, config.carbon_column, config.forecast_horizon_hours)
    start_range, end_range = config.start_epoch, config.end_epoch
    randranges = []
    while len(randranges) < 10:
        randrange = random.randint(start_range, end_range)

        for period in cidservice.yield_lowest_periods(datetime.fromtimestamp(randrange, tz=UTC), 30*60):
            print(period)
            break

        randrange = get_rounded_iso_range(randrange, randrange + 30*60)
        
        if randrange not in randranges:
            randranges.append(randrange)
    print(cidservice.get_climate_data(*randrange))