# On Slurm (and Lotus)'s default scheduling algorithm
* By default:
	* Slurm uses a First-In-First-Out (FIFO) algorithm.
	* Jobs are evaluated strictly in their order of priority (which defaults to submission time).
* The problem is here is high-priority jobs that cannot run because resources are unavailable will prevent any lower-priority jobs from running and stall the queue.
* As a results, clusters usually two additional options:
	* `sched/backfill` (instead of `sched/builtin`) is backfill scheduling, which allows smaller, lower-priority jobs to skip ahead in line and utilize idle resources. It will only do this if launching the smaller job does not delay the projected start time of the higher-priority jobs ahead of it.
	* `priority/multifactor` (instead of `priority/basic`, which is FIFO) can be used to allow multiple factors, such as job age, fairshare (adjusts the job priority based on a user's historical consumption) and quality of service (QOS) tiers.
* JASMIN's LOTUS cluster has made several configuration changes:
	* Priority is heavily weighted by fairshare (so historical consumption by 1,000,000), less so for QOS (1000) and none whatsoever for age (being in the queue longer) or by its size (so if it's a large or small job)
	* The scheduling system is composed of two parts, a main scheduler and the backfill scheduler:
		* The main scheduler is event-driven, running whenever a job is submitted, finishes or a node becomes free. However, for LOTUS, this is limited to running only every 20 seconds to allowing batching up along with a minimum 2 second interval (otherwise the system would lock up if hundreds of jobs finish at the same time). It can only look at the top 2000 jobs globally and top 500 jobs per user.
		* The backfill scheduler is set to run every 5 minutes, it only looks 7 days into the future and is limited to processing 500 jobs per user and 5,000 jobs globally every 5 minutes. 
	* Effectively, both schedulers look at the same queue, going down the queue which is sorted in priority order. The main scheduler stops when it can't allocate a job, whereas the backfill scheduler looks at the whole queue to see if smaller jobs can be slotted in.
