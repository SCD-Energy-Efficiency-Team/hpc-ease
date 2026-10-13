import bisect
import csv
import logging
import math
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from pathlib import Path

import dataset
import requests
from dotenv import dotenv_values

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

BASE = "https://api.carbonintensity.org.uk"

FORECAST_HORIZON = timedelta(hours=96)

YIELD_CACHE_SIZE = 4096
CLIMATE_CACHE_SIZE = 512

project_root = Path(__file__).resolve().parent.parent

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

def parse_time_inputs(start, end):
    assert type(start) is type(end)
    thetype = type(start)
    if thetype is str:
        return start, end
    elif thetype is int:
        return get_rounded_iso_range(start, end)
    else:
        raise Exception(thetype)

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

class CarbonIntensity:
    def _get_climate_data(self, start, end, postcode="OX11", returnvalues="forecast"):
        # returns {(start_datetime, end_datetime): intensityint, ...}
        raise NotImplementedError

    def yield_lowest_periods(self, start_datetime, duration: int, postcode="OX11"):
        start_datetime = min(start_datetime, datetime.now(tz=UTC))
        epoch = start_datetime.timestamp()
        return self._yield_lowest_periods_cached(
            round_to_30min(epoch, False, iso=False), epoch % 1800 == 0, duration, postcode
        )

    def _yield_lowest_periods(self, floored_epoch, is_grid_aligned, duration: int, postcode="OX11"):
        # returns a list, with each item being:
        # (window_intensityint, start_datetime, end_datetime)

        limitstart = gen_iso_string(floored_epoch)
        limitend = gen_iso_string(floored_epoch + int(FORECAST_HORIZON.total_seconds()) + (0 if is_grid_aligned else 1800))
        carbon_intensity = self.get_climate_data(limitstart, limitend, postcode, "forecast")
        carbon_prefix = build_intensity_prefix(carbon_intensity)
        carbon_cumsum = build_intensity_cumsum(carbon_prefix)
        window_start = datetime.fromisoformat(limitstart)
        window_end = window_start + timedelta(seconds=duration)

        # TODO: This might break in the case duration is less than the interval between window start and end?
        windows = defaultdict(list)
        while window_end < datetime.fromisoformat(limitend):
            intensity, covered = calculate_cumulative_intensity(window_start, window_end, carbon_prefix, carbon_cumsum)
            
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

class CarbonIntensitySQL(CarbonIntensity):
    def __init__(self, db):
        self.db = db
        self._yield_lowest_periods_cached = lru_cache(maxsize=YIELD_CACHE_SIZE)(self._yield_lowest_periods)
        self.get_climate_data = lru_cache(maxsize=CLIMATE_CACHE_SIZE)(self._get_climate_data)

    def fetch_climate_data_pure(self, start, end, postcode="OX11"):
        logger.info(f"Downloading carbon intensity data between {start} and {end}...")
        start, end = parse_time_inputs(start, end)

        url = f"{BASE}/regional/intensity/{start}/{end}/postcode/{postcode}"
        data = requests.get(url).json()
        now_datetime = datetime.now(UTC)
        data = data["data"]
        regionid, shortname = data["regionid"], data["shortname"]
        data = data["data"]

        self.db["regions"].upsert({"regionid": regionid, "shortname": shortname}, ["regionid"])
        self.db["postcodes"].upsert({"postcode": postcode, "regionid": regionid, "shortname": shortname}, ["postcode"])

        for row in data:
            timerange = f"{row['from']},{row['to']}"
            typefield = datetime.fromisoformat(row["to"]) > now_datetime
            typefield = "forecast" if typefield else "actual"
            basedict = {"timerange": timerange, "type": typefield}
            intensity = basedict.copy() | row["intensity"]
            generationmix = basedict.copy() | {gendict["fuel"]: gendict["perc"] for gendict in row["generationmix"]}
            self.db[f"region{regionid}_intensity"].upsert(intensity, ["timerange"])
            self.db[f"region{regionid}_generationmix"].upsert(generationmix, ["timerange"])

    def fetch_climate_data(self, start, end, postcode="OX11"):
        start, end = parse_time_inputs(start, end)

        region_row = self.db["postcodes"].find_one(postcode=postcode)
        if region_row is None:
            url = f"{BASE}/regional/postcode/{postcode}"
            data = requests.get(url).json()["data"][0]
            regionid, shortname = data["regionid"], data["shortname"]
            self.db["regions"].upsert({"regionid": regionid, "shortname": shortname}, ["regionid"])
            self.db["postcodes"].upsert({"postcode": postcode, "regionid": regionid, "shortname": shortname}, ["postcode"])
        else:
            regionid = region_row["regionid"]
        tablebasename = f"region{regionid}"

        limitstart, limitend = datetime.fromisoformat(start), datetime.fromisoformat(end)
        start = limitstart
        ranges = []
        while True:
            end = start + timedelta(minutes=30)
            iso_start, iso_end = gen_iso_string(start.timestamp()), gen_iso_string(end.timestamp())
            row = self.db[tablebasename+"_intensity"].find_one(timerange=f"{iso_start},{iso_end}")
            if row is None or row["type"] in [None, "forecast"]:
                if len(ranges) == 0:
                    ranges.append([iso_start, iso_end])
                else:
                    if ranges[-1][1] == iso_start:
                        ranges[-1][1] = iso_end
                    else:
                        ranges.append([iso_start, iso_end])
            if end.timestamp() >= limitend.timestamp():
                break
            start += timedelta(minutes=30)

        for arange in ranges:
            self.fetch_climate_data_pure(*arange, postcode)
    
    def _get_climate_data(self, start, end, postcode="OX11", returnvalues="forecast"):
        # returnvalues can be "forecast", "index" or "both"
        # "forecast" is carbon intensity (in gco2/kwh) and index is "very low"/"low"/etc.
        assert returnvalues in ["forecast", "index", "both"]
        start, end = parse_time_inputs(start, end)

        self.fetch_climate_data(start, end, postcode)
        region_row = self.db["postcodes"].find_one(postcode=postcode)
        assert region_row is not None
        tablebasename = f"region{region_row['regionid']}"

        limitstart, limitend = datetime.fromisoformat(start), datetime.fromisoformat(end)
        start = limitstart
        results = {}
        while True:
            end = start + timedelta(minutes=30)
            iso_start, iso_end = gen_iso_string(start.timestamp()), gen_iso_string(end.timestamp())
            intensity_row = self.db[tablebasename+"_intensity"].find_one(timerange=f"{iso_start},{iso_end}")
            if intensity_row is None:
                raise MissingCarbonData(f"Missing data for {start} to {end}")
            forecast, index = intensity_row["forecast"], intensity_row["index"]
            key = (start, end)
            if returnvalues == "forecast":
                results[key] = forecast
            elif returnvalues == "index":
                results[key] = index
            elif returnvalues == "both":
                results[key] = [forecast, index]
            else:
                raise Exception(returnvalues)
            if gen_iso_string(limitend.timestamp()) == iso_end:
                break
            start += timedelta(minutes=30)
        return results

class CarbonIntensitySQLite(CarbonIntensitySQL):
    def __init__(self):
        super().__init__(
            dataset.connect((project_root / "carbondata.db").as_uri().replace("file:", "sqlite:"))
        )


class CarbonIntensityMariaDB(CarbonIntensitySQL):
    def __init__(self):
        creds = dotenv_values(project_root / ".env")
        db_url = f"mariadb+mariadbconnector://{creds['DB_USERNAME']}:{creds['DB_PASSWORD']}@/{creds['PROCESSED_DATABASE']}?unix_socket=/var/lib/mysql/mysql.sock"
        db = dataset.connect(db_url)
        super().__init__(db)

class CarbonIntensityCSV(CarbonIntensity):
    def __init__(self):
        self._yield_lowest_periods_cached = lru_cache(maxsize=YIELD_CACHE_SIZE)(self._yield_lowest_periods)
        self.get_climate_data = lru_cache(maxsize=CLIMATE_CACHE_SIZE)(self._get_climate_data)

        self.csv_data = None
        self.cache_csv_data()

    def cache_csv_data(self):
        self.csv_data = {}
        with open(project_root / "main/regional_carbon_intensity.csv", mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                start = datetime.fromisoformat(row["datetime"]).replace(tzinfo=UTC)
                end = start + timedelta(minutes=30)
                value = row["South England"]
                if not value:
                    continue
                self.csv_data[(start, end)] = float(value)

    def _get_climate_data(self, start, end, postcode="OX11", returnvalues="forecast"):
        # returns {(start_datetime, end_datetime): intensityint, ...}
        assert postcode.startswith("OX"), "Region lookup is currently not supported"
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
    cidservice = CarbonIntensityCSV()
    start_range, end_range = 1725608400, 1780351201
    randranges = []
    while len(randranges) < 10:
        randrange = random.randint(start_range, end_range)

        for period in cidservice.yield_lowest_periods(datetime.fromtimestamp(randrange, tz=UTC), 30*60, "OX11"):
            print(period)
            break

        randrange = get_rounded_iso_range(randrange, randrange + 30*60)
        
        if randrange not in randranges:
            randranges.append(randrange)
    print(cidservice.get_climate_data(*randrange, "OX11"))