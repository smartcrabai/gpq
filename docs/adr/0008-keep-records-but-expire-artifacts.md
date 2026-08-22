# Keep records but expire Artifacts

PostgreSQL retains Generation and Attempt metadata, prompts, workflow payloads, final LLM text, usage, errors, state transitions, and latest progress snapshots indefinitely, but not token deltas, transport frames, previews, or large binary Artifacts. Native API input Artifacts use S3-compatible ephemeral placement so queued work can move between Workers; synchronous OpenAI image inputs may relay directly while the request remains connected. Output Artifacts choose Worker-local relay or S3-compatible ephemeral placement. Inputs are deleted when the Generation terminates; outputs are deleted when their one-shot transfer completes or disconnects, and unclaimed outputs expire one hour after completion. Only Remote holds S3 credentials and issues object-scoped 15-minute presigned URLs to leased Workers.

Worker-local delivery uses bounded one-MiB gRPC chunks with declared size and SHA-256 validation and no additional compression. An interrupted internal Worker stream may resume by delivery token and offset while the external client remains connected; external client disconnection ends the one-shot delivery and deletes the output. S3 transfers use multipart operations.

Artifacts transition independently through `Pending`, `Available`, `Delivering`, and exactly one of `Consumed`, `Expired`, or `Lost`. Generation success never rolls back when its Artifact later disappears; a second concurrent download conflicts, consumed or unavailable terminal Artifacts return `410 Gone`, and a temporarily offline Worker yields a retryable unavailable response.

Worker-local outputs are crash-recoverable filesystem state, not a local database: each Artifact directory contains atomically published data and a final manifest, is scanned and reconciled on Worker startup, and is deleted after Remote acknowledgement or expiry. Loss of the state directory marks remaining local Artifacts `Lost`.

S3 configuration is optional and does not affect Remote readiness. Without it, text-only work, synchronous OpenAI image relay, and Worker-local outputs remain available, while Native input Artifacts and S3 output placement fail admission explicitly.
