import csv
import hashlib
import json
from pathlib import Path

keys = ["chosen_day", "days_to_fetch", "jobs_file", "num_jobs", "exit_code", "error_type", "elapsed_sec", "max_ram_kib", "energy", "energy_2", "carbon", "carbon_2", "sys_time_sec", "user_time_sec", "unschedulable_jobs", "unschedulable_energy"]

def process(filename):
    rows = []
    with open(filename) as file:
        for rawline in file:
            run = json.loads(rawline)
            error_level = run["E-Level"]
            state = run["State"].rstrip("+")
            output = run["Output"].strip().split("\n")
            cmd = output[0].split(".py ", 1)[1]

            result = {}
            if state in ("TIMEOUT", "CANCELLED", "NODE_FAIL"):
                result["error_type"] = {
                    "TIMEOUT": "timeout",
                    "CANCELLED": "cancelled",
                    "NODE_FAIL": "node_fail",
                }[state]
            elif error_level in [0, "0:0"]:
                for outline in output[-2:]:
                    result |= json.loads(outline)
            else:
                result |= json.loads(output[-1])
                joined_output = "\n".join(output)
                if '❱ 59 │   │   │   assert job["req"]["node"] == 1' in joined_output and "AssertionError" in joined_output:
                    result["error_type"] = "more_than_one_node"
                elif "MissingCarbonData: Missing data for" in joined_output:
                    result["error_type"] = "missing_carbon_data"
                elif "VirtualCapacityExhausted" in joined_output or "❱ 70 │   │   │   assert len(available) == num_nodes_req" in joined_output:
                    result["error_type"] = "virtual_capacity_exhausted"
                elif "Parquet magic bytes not found in footer" in joined_output:
                    result["error_type"] = "parquet_corrupted_db"
                elif "OverflowError: date value out of range" in joined_output:
                    # fixed at the source; kept for reprocessing pre-fix temp/ runs
                    result["error_type"] = "timelimit_overflow"
                elif (
                    ("this a 'parquet' file?: Failed to open local file" in joined_output
                     and "Detail: [errno 2] No such file or directory" in joined_output)
                    or ("FileNotFoundError" in joined_output and "_ensure_single_source" in joined_output)
                ):
                    result["error_type"] = "parquet_went_missing"
                else:
                    print("\n".join(output))
                    raise Exception()

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
            
            if "exit_code" in result:
                assert error_level == result["exit_code"] or error_level == f"{result['exit_code']}:0", f"{error_level!r} {result['exit_code']!r}"

            if "energy" in result:
                pass
                # assert math.isclose(result["energy"], result["energy_2"]), f"{result['energy']} {result['energy_2']}"

            rows.append(result)

    for row in rows:
        for key in row:
            assert key in keys, key

    if all(row["chosen_day"].endswith("T00:00:00+00:00") for row in rows):
        for row in rows:
            row["chosen_day"] = row["chosen_day"].rsplit("T", 1)[0]

    serialized_data = json.dumps({"keys": keys, "rows": rows}, sort_keys=True)
    hashofdata = hashlib.sha256(serialized_data.encode("utf-8")).hexdigest()
    with open(f"processed/{filename.stem}-{hashofdata[0:12]}.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)

if __name__ == "__main__":
    for filename in Path("temp").glob("*.txt"):
        process(filename)
