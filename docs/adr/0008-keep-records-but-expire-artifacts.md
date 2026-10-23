# Keep records but expire Artifacts

PostgreSQL retains Generation and Attempt metadata, prompts, errors, state transitions, and latest progress snapshots; binary Artifact content has a separate lifecycle. Image and Video inputs can use `RemoteLocal`, storing bytes in PostgreSQL `bytea` and allowing Worker staging without S3. Unclaimed inputs expire after one hour; inputs claimed by a Generation remain available until its terminal cleanup. Outputs use `WorkerLocal` or optional `ObjectStore` placement, expire one hour after completion unless consumed, and are delivered once. Only Remote holds S3 credentials for object-store transfers.

Worker Artifact transfers validate declared size and SHA-256. Worker-local outputs retain their Worker identity and delivery token for retrieval; input and output Artifact state transitions remain independent of Generation state.

Artifacts transition through `Pending`, `Available`, `Delivering`, and exactly one of `Consumed`, `Expired`, or `Lost`. Generation success never rolls back when an output Artifact later disappears; concurrent downloads conflict, and consumed, expired, or lost outputs return `410 Gone`.

Worker-local outputs are crash-recoverable filesystem state, not a local database: each Artifact directory contains atomically published data and a final manifest, is scanned and reconciled on Worker startup, and is deleted after Remote acknowledgement or expiry. Loss of the state directory marks remaining local Artifacts `Lost`.

S3 configuration is optional and does not affect Remote readiness. Without it, PostgreSQL-backed `RemoteLocal` inputs and `WorkerLocal` outputs remain available; `ObjectStore` placement requires S3.
