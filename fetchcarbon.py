import dataset
import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from functools import lru_cache
import math

BASE = "https://api.carbonintensity.org.uk"
db = dataset.connect("sqlite:///carbondata.db")

def gen_iso_string(epoch_time):
    return datetime.fromtimestamp(epoch_time, tz=timezone.utc).strftime("%Y-%m-%dT%H:%MZ")

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

def fetch_climate_data_pure(start, end, postcode="OX11"):
    print(f"Downloading carbon intensity data between {start} and {end}...")
    start, end = parse_time_inputs(start, end)

    url = f"{BASE}/regional/intensity/{start}/{end}/postcode/{postcode}"
    data = requests.get(url).json()
    now_datetime = datetime.now(timezone.utc)
    data = data["data"]
    regionid, shortname = data["regionid"], data["shortname"]
    data = data["data"]

    db["regions"].upsert(dict(regionid=regionid, shortname=shortname), ["regionid"])
    db["postcodes"].upsert(dict(postcode=postcode, regionid=regionid, shortname=shortname), ["postcode"])

    for row in data:
        timerange = f"{row['from']},{row['to']}"
        typefield = datetime.fromisoformat(row["to"]) > now_datetime
        typefield = "forecast" if typefield else "actual"
        basedict = {"timerange": timerange, "type": typefield}
        intensity = basedict.copy() | row["intensity"]
        generationmix = basedict.copy() | {gendict["fuel"]: gendict["perc"] for gendict in row["generationmix"]}
        db[f"region{regionid}_intensity"].upsert(intensity, ["timerange"])
        db[f"region{regionid}_generationmix"].upsert(generationmix, ["timerange"])

def fetch_climate_data(start, end, postcode="OX11"):
    start, end = parse_time_inputs(start, end)

    region_row = db["postcodes"].find_one(postcode=postcode)
    if region_row is None:
        url = f"{BASE}/regional/postcode/{postcode}"
        data = requests.get(url).json()["data"][0]
        regionid, shortname = data["regionid"], data["shortname"]
        db["regions"].upsert(dict(regionid=regionid, shortname=shortname), ["regionid"])
        db["postcodes"].upsert(dict(postcode=postcode, regionid=regionid, shortname=shortname), ["postcode"])
    else:
        regionid = region_row["regionid"]
    tablebasename = f"region{regionid}"

    limitstart, limitend = datetime.fromisoformat(start), datetime.fromisoformat(end)
    start = limitstart
    ranges = []
    while True:
        end = start + timedelta(minutes=30)
        iso_start, iso_end = gen_iso_string(start.timestamp()), gen_iso_string(end.timestamp())
        row = db[tablebasename+"_intensity"].find_one(timerange=f"{iso_start},{iso_end}")
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
        fetch_climate_data_pure(*arange, postcode)

cache = {}

def get_climate_data(start, end, postcode="OX11", returnvalues="forecast"):
    # returnvalues can be "forecast", "index" or "both"
    # "forecast" is carbon intensity (in gco2/kwh) and index is "very low"/"low"/etc.
    assert returnvalues in ["forecast", "index", "both"]
    start, end = parse_time_inputs(start, end)

    cache_key = (start, end, postcode, returnvalues)
    if cache_key in cache:
        return cache[cache_key]

    fetch_climate_data(start, end, postcode)
    region_row = db["postcodes"].find_one(postcode=postcode)
    assert region_row is not None
    tablebasename = f"region{region_row['regionid']}"

    limitstart, limitend = datetime.fromisoformat(start), datetime.fromisoformat(end)
    start = limitstart
    results = {}
    while True:
        end = start + timedelta(minutes=30)
        iso_start, iso_end = gen_iso_string(start.timestamp()), gen_iso_string(end.timestamp())
        intensity_row = db[tablebasename+"_intensity"].find_one(timerange=f"{iso_start},{iso_end}")
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

    cache[cache_key] = results
    return results

def build_intensity_prefix(intensity_data):
    prefix = []

    for (start, end), intensity in sorted(intensity_data.items()):
        prefix.append((start, end, intensity))

    return prefix

def calculate_cumulative_intensity(window_start, window_end, prefix):
    total = 0.0
    covered = timedelta(0)

    for period_start, period_end, intensity in prefix:
        if period_end <= window_start:
            continue
        if period_start >= window_end:
            break

        overlap_start = max(window_start, period_start)
        overlap_end = min(window_end, period_end)

        overlap = overlap_end - overlap_start
        covered += overlap
        total += intensity * overlap.total_seconds() / 3600

    return total, covered

@lru_cache(maxsize=100)
def yield_lowest_periods(start_datetime, duration, postcode="OX11"):
    start_datetime = min(start_datetime, datetime.now())
    end_datetime = start_datetime + timedelta(hours=96)
    limitstart, limitend = get_rounded_iso_range(start_datetime.timestamp(), end_datetime.timestamp())
    carbon_intensity = get_climate_data(limitstart, limitend, postcode, "forecast")
    carbon_prefix = build_intensity_prefix(carbon_intensity)
    window_start = datetime.fromisoformat(limitstart)
    window_end = window_start + timedelta(seconds=duration)

    # TODO: This might break in the case duration is less than the interval between window start and end?
    windows = defaultdict(list)
    while window_end < datetime.fromisoformat(limitend):
        intensity, covered = calculate_cumulative_intensity(window_start, window_end, carbon_prefix)
        
        if covered != timedelta(seconds=duration):
            break

        windows[intensity].append(window_start)

        window_start += timedelta(minutes=1)
        window_end = window_start + timedelta(seconds=duration)

    results = []
    for value in sorted(windows):
        for window_start in windows[value]:
            results.append((value, window_start, window_start + timedelta(seconds=duration)))
    return results

if __name__ == "__main__":
    # print(fetch_climate_data("2026-06-01T00:00Z", "2026-06-05T00:00Z", "OX11"))
    print(get_climate_data("2026-06-02T00:30Z", "2026-06-04T10:30Z", "OX11"))