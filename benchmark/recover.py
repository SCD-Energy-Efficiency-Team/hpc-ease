import json
import re
import subprocess
import time
from pathlib import Path

from runmultiple import is_terminal

CHUNK = 200

# rebuilds temp/ logs from slurmoutputs/ after runmultiple.py died before writing its own, one file per distinct command line (date aside); leaves the job logs in place
def main():
    logs = sorted(Path("slurmoutputs").glob("job_*.log"))
    print(f"Found {len(logs)} job logs in slurmoutputs/")

    states = {}
    for start in range(0, len(logs), CHUNK):
        ids = [log.stem.removeprefix("job_") for log in logs[start:start + CHUNK]]
        out = subprocess.run(
            ["sacct", "-X", "-n", "-P", "--format=JobID,State,ExitCode,Elapsed", "-j", ",".join(ids)],
            capture_output=True, text=True, check=True,
        ).stdout
        for line in out.splitlines():
            job_id, state, elevel, elapsed = line.split("|")
            if job_id.isdigit():
                states[job_id] = (state.split()[0], elevel, elapsed)

    by_command = {}
    skipped = 0
    for log in logs:
        job_id = log.stem.removeprefix("job_")
        if job_id not in states or not is_terminal(states[job_id][0]):
            skipped += 1
            continue
        state, elevel, elapsed = states[job_id]
        output = log.read_text()
        command = re.sub(r"--chosen-day \S+", "--chosen-day DATE", output.split("\n", 1)[0])
        row = {"ID": int(job_id), "State": state, "E-Level": elevel, "Time": elapsed, "Output": output}
        by_command.setdefault(command, []).append(row)

    stamp = int(time.time() * 1000)
    for index, (command, rows) in enumerate(by_command.items(), 1):
        filename = Path("temp") / f"{stamp}-recovered-{index}.txt"
        with open(filename, "x") as file:
            for row in rows:
                file.write(json.dumps(row) + "\n")
        print(f"Wrote {len(rows)} rows to {filename}\n    {command}")
    print(f"Skipped {skipped} logs that were unfinished or unknown to sacct")

if __name__ == "__main__":
    main()
