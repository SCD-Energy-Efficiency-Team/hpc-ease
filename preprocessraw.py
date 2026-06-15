from pathlib import Path
import json
from tqdm import tqdm

for filename in tqdm(Path("prodjobs/raw").glob("*.json")):
    with open(filename) as file:
        data = json.loads(file.read())
    newpath = Path(str(filename).replace("raw", "lists")+"l")
    with open(newpath, "w") as file:
        for job in data["jobs"]:
            file.write(json.dumps(job)+"\n")
