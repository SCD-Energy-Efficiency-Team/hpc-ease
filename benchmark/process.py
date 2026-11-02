import csv
import hashlib
import json
from pathlib import Path
from tqdm import tqdm

def process(filename):
    rows = []
    with open(filename) as file:
        for rawline in file:
            run = json.loads(rawline)
            error_level = run["E-Level"]
            state = run["State"].rstrip("+")
            output = run["Output"].strip().split("\n")

            result = {}
            if state in ("RUNNING", "PENDING", "REQUEUED"):
                # log was gathered before the job finished: no result line to parse
                result["error_type"] = "still_running"
            elif state in ("TIMEOUT", "CANCELLED", "NODE_FAIL", "OUT_OF_ME"):
                result["error_type"] = {
                    "TIMEOUT": "timeout",
                    "CANCELLED": "cancelled",
                    "NODE_FAIL": "node_fail",
                    "OUT_OF_ME": "out_of_memory",  # sacct truncates OUT_OF_MEMORY
                }[state]
            elif output[0].endswith("/usr/bin/time: No such file or directory"):
                result["error_type"] = "time_binary_missing"
            elif error_level in [0, "0:0"]:
                for outline in output[-2:]:
                    result |= json.loads(outline)
            else:
                result |= json.loads(output[-1])
                joined_output = "\n".join(output)
                if "MissingCarbonData: Missing data for" in joined_output:
                    result["error_type"] = "missing_carbon_data"
                elif "VirtualCapacityExhausted" in joined_output:
                    result["error_type"] = "virtual_capacity_exhausted"
                elif "Parquet magic bytes not found in footer" in joined_output:
                    result["error_type"] = "parquet_corrupted_db"
                elif (
                    "this a 'parquet' file?: Failed to open local file" in joined_output
                    and "Detail: [errno 2] No such file or directory" in joined_output
                ):
                    result["error_type"] = "parquet_went_missing"
                else:
                    print("\n".join(output))
                    raise Exception()

            cmd = output[0].split(".py ", 1)[1] if ".py " in output[0] else ""

            for param in ["--jobs-file", "--chosen-day", "--days-to-fetch"]:
                if param in cmd:
                    value = cmd.split(param+" ", 1)[1].split(" ")[0].strip()
                    param = param.removeprefix("--").replace("-", "_")
                    if param == "chosen_day" and "T" not in value:
                        value += "T00:00:00+00:00"
                    elif param == "days_to_fetch":
                        value = int(value)
                    if param not in result:
                        result[param] = value
                
                if "num_jobs" not in result:
                    for line in output:
                        splittext = "INFO: Total number of jobs: "
                        if splittext in line:
                            result["num_jobs"] = int(line.split(splittext)[1].strip())
            
            # logs from before retirement was always on carry --retire/--no-retire; newer ones carry neither
            if "--no-retire" in cmd:
                result["retire"] = False
            elif "--retire" in cmd:
                result["retire"] = True
            # --max-wait was called --max-delay in older logs
            for max_wait_flag in ("--max-wait ", "--max-delay "):
                if max_wait_flag in cmd:
                    result["max_wait"] = float(cmd.split(max_wait_flag, 1)[1].split(" ")[0])
                    break
            if "--reschedule-percent " in cmd:
                result["reschedule_percent"] = float(cmd.split("--reschedule-percent ", 1)[1].split(" ")[0])
            if "--reschedule-seed " in cmd:
                result["reschedule_seed"] = int(cmd.split("--reschedule-seed ", 1)[1].split(" ")[0])
            if "--warmup-days " in cmd:
                result["warmup_days"] = float(cmd.split("--warmup-days ", 1)[1].split(" ")[0])
            if "--cooldown-days " in cmd:
                result["cooldown_days"] = float(cmd.split("--cooldown-days ", 1)[1].split(" ")[0])
            # logs from before defer became the default carry --defer or neither flag (defer off); newer ones carry one explicitly
            if "--no-defer" in cmd:
                result["defer"] = False
            elif "--defer" in cmd:
                result["defer"] = True
            if "--retry-timeout " in cmd:
                result["retry_timeout"] = float(cmd.split("--retry-timeout ", 1)[1].split(" ")[0])
            # blank means process.py's default (4.0), since the harness omits the flag when --head-weight is not given
            if "--head-weight " in cmd:
                result["head_weight"] = float(cmd.split("--head-weight ", 1)[1].split(" ")[0])
            # blank means process.py's default (0.25)
            if "--head-fraction " in cmd:
                result["head_fraction"] = float(cmd.split("--head-fraction ", 1)[1].split(" ")[0])

            if "exit_code" in result:
                assert error_level == result["exit_code"] or error_level == f"{result['exit_code']}:0", f"{error_level!r} {result['exit_code']!r}"

            if "energy" in result:
                pass
                # assert math.isclose(result["energy"], result["energy_2"], abs_tol=1.0), f"{result['energy']} {result['energy_2']} {filename}"

            rows.append(result)

    keys = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)

    if all(row.get("chosen_day", "").endswith("T00:00:00+00:00") for row in rows if "chosen_day" in row):
        for row in rows:
            if "chosen_day" in row:
                row["chosen_day"] = row["chosen_day"].rsplit("T", 1)[0]

    serialized_data = json.dumps({"keys": keys, "rows": rows}, sort_keys=True)
    hashofdata = hashlib.sha256(serialized_data.encode("utf-8")).hexdigest()
    Path("processed").mkdir(exist_ok=True)
    with open(f"processed/{filename.stem}-{hashofdata[0:12]}.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)

if __name__ == "__main__":
    for filename in tqdm(list(Path("temp").glob("*.txt"))):
        process(filename)
