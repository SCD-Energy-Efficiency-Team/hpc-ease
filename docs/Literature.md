# Squirrel

## How it works
* Squirrel does not modify Slurm, it acts a wrapper much like CATS where the user calls squirrel with their job file, it runs the script with a modified sbatch command.
* Squirrel basically:
    * Gets historical/forecasted carbon intensity data from an internal InfluxDB database
    * Looks up in a user provided configuration file the TDP of the CPUs and GPUs for every node in the cluster.
    * Using a specific algorithm (such as temporal) it identifies the best time window and node for the job.
    * It tracks its own reservations via a CSV file so it maintain its own version of the cluster state and can allocate jobs on different nodes depending on this.
* The sbatch command sets three parameters: `--begin` (the time it wants to run the job), `--nodelist` (only one node is chosen) and `--exclusive` (which ensures only one job runs per node so Squirrel can measure energy and carbon savings)
* Squirrel chooses the nodes with the lowest TDP first, it also does not manage nodes' power states itself, whatever nodes it is not using it leaves to Slurm to recognise these nodes as being idle and powering them down.

## Limitations
* The authors state Squirrel is not currently made for multi-user scenarios, given it maintains its own view of the cluster's state via a CSV file that tracks its own reservations - it does not query the central Slurm database which is what the authors proposed as an extension.
* It runs one job per node via `--exclusive`, because the authors wished to measure overhead energy consumption to see if emissions were reduced. They proposed to solve this using TDP per core multiplied by the number of cores used to estimate a job's share of the energy, or technologies like NVIDIA Multi-Instance GPU could allow a single GPU to be partitioned into isolated instances that each have a predictable power profile.
* It does not recompute schedules if a job fails or finishes earlier than expected, so technically cluster utilization could be made more optimal.
* They used fixed one-hour timeslots to match the resolution of the grid carbon intensity data. If a job finishes in 15 minutes, the node remains "blocked" in Squirrel's timetable for the remaining 45 minutes, resulting in wasted resources and lost savings.
* The carbon intensity forecast is limited to 24 hours ahead, which restricts the maximum runtime for any single job (this would likely be an issue that would also affect our scenario, although it would likely affect us less as it forecasts 96 hours/4 days ahead)
* It assumes a data centre's PUE is static, but this would change throughout the day based on cooling efficiency and ambient temperature.
* It only focuses on scope 2 emissions and doesn't take into account the embodied carbon of the hardware itself, which might lead the system to favour newer, more efficient nodes even if extending the life of older hardware would result in lower total lifecycle emissions.

