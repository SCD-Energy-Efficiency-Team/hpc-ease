# How it works

We essentially have two main scripts in the codebase:

- **main/process.py** - This is the simulator. It takes a window of historical Slurm jobs to directly answer the question "how much carbon would rescheduling this window have saved?". For a single (start date, span) pair it:
  1. Reads the jobs from a Parquet export of the historical database.
  2. Computes the energy of each job from the linear utilisation-based node power model, scaled by the share of a node's cores the job held.
  3. Computes the original carbon as energy times the grid carbon intensity during the period the job actually ran.
  4. Replays the same jobs through a simulated cluster of identical nodes. Each job is placed in the lowest-intensity window that has capacity within the 96-hour forecast horizon. Elapsed time and energy are preserved, so only the timing changes.
  5. Compares the two schedules and reports the saving, along with wait times, deferred and pinned job counts, and capacity violations.

  It takes in several flags which can alter execution, these are described later in its section. Other files in main include:
  1. *fetchcarbon.py* to fetch the carbon intensity data from a [CSV export](https://www.neso.energy/data-portal/regional-carbon-intensity-forecast) of the [UK Carbon Intensity API](https://carbonintensity.org.uk) (the version in this repository has been patched to fill in missing gaps with estimated data).
  2. *fetchjobs.py* reads the Parquet export of the historical database and yields each job in one-minute submit-time batches so the simulator sees them in arrival order.
  3. *scheduling.py* holds the actual scheduling code which models the virtual cluster and runs the alternative algorithm.
  4. *datasetconfig.py* loads and validates *dataset.toml* which describes parameters related to the dataset like the cluster's dimensions, the power model constants, the carbon data source and the dataset's date range.
  5. *toparquet.py* is a standalone script that connects to and exports the raw Slurm historical database to the Parquet file, so a simulation never needs database access.

- **benchmark/runmultiple.py** - It does not simulate anything itself, but it runs `process.py` many times and manages the runs. It:
  1. Snapshots the code (optionally at a given commit) so that the code under test stays frozen during a sweep.
  2. Enumerates the (date x span) grid, an example would be every day from 2025-02-01 at spans of 1 to 90 days, and drops windows that would run past the dataset end.
  3. Submits each run as an independent single-core job, through Slurm or Task Spooler (run locally), with memory and time sized from the streamed span.
  4. Polls until the queue is empty and collects each run's JSON result line, plus its runtime and peak RAM from the GNU Time utility.

  A *clusters.toml* file allows you to specify the Slurm cluster they wish to run benchmark jobs on and parameters to do so (such as account, partition, QOS and resource limits). Multiple Slurm clusters can be configured and selected with the `--route` parameter. Once finished, *runmultiple.py* outputs raw logs to a folder, which *benchmark/process.py* turns into aggregated CSVs, and *compare.py* analyses these pair-wise in turn as a form of A/B benchmark testing.