# Prioritize GPU utilization

Scheduling first excludes Generations incompatible with the Tenant, exact Model and Workflow Versions, or available Slot capacity. It then chooses overdue work by oldest `created_at`; otherwise it favors the Slot's resident Model and compatible batches, then higher priority, then older submission time. Running Attempts are never preempted for priority. This cache-aware order prioritizes aggregate GPU utilization while the Tenant-configurable `maximum_queue_age`, defaulting to 30 minutes, prevents indefinite starvation.
