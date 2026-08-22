# Use leased at-least-once execution

Generations use leased at-least-once delivery because a remote coordinator cannot prove whether a disconnected Worker started or completed GPU work. Each retry creates a distinct Attempt; the first successfully committed result becomes the Accepted Result, and later duplicate results cannot replace it. Automatic retries stop after three Attempts and apply only to Worker loss, lease expiry, backend crashes, transient transfers, and candidate-specific runtime OOM; invalid input, missing models, and unsupported features fail immediately. Manual retry creates a new Generation. Workers heartbeat every 10 seconds against a 45-second lease, may resume the same Attempt after reconnecting before expiry, and must cooperatively cancel after expiry; results committed under an expired lease are rejected.

Generation states are `Queued`, `Running`, `Cancelling`, `Succeeded`, `Failed`, `Cancelled`, and `Expired`; Attempt states are `Leased`, `Running`, `Succeeded`, `Failed`, `Cancelled`, and `LeaseExpired`. Transitions are monotonic, retries create new Attempts, and output delivery does not delay a Generation's `Succeeded` transition.

Capability mismatches discovered before execution do not create Attempts. A runtime OOM fails the Attempt, invalidates that Slot's claimed Model or Workflow capability, counts against the retry budget, and may retry on another eligible Slot; the Generation fails for VRAM insufficiency only after every registered candidate is known incapable.

Result acceptance is a PostgreSQL compare-and-set transaction that verifies the live lease, a nonterminal Generation, and no prior Accepted Result before atomically marking the Attempt and Generation succeeded. S3 outputs are uploaded and checksum-verified first; stale or duplicate commit rejection requires the Worker to delete its output.

Queued cancellation terminates immediately; running cancellation enters `Cancelling` until Worker acknowledgement. Cancellation acknowledgement and result commitment race through terminal compare-and-set, so whichever commits first wins and the loser observes the final state. Repeated cancellation is idempotent.

Attempt execution defaults to 30 minutes for LLM, two hours for images, and 24 hours for video or music; a Model or Workflow Version may lower or replace its default, and a Generation may only shorten it. Execution timeout begins at `Running`, is not retried, and triggers cooperative cancellation followed by backend restart when needed.

Workers normalize backend failures to `InvalidInput`, `UnsupportedCapability`, `ModelUnavailable`, `OutOfMemory`, `BackendCrashed`, `ExecutionTimedOut`, `Cancelled`, `TransferFailed`, or `Internal`, alongside a retry hint. Remote applies the authoritative retry policy from the enum rather than parsing backend text; raw errors remain diagnostic data only.

Each Generation internally records whether its caller is synchronous or durable. On Remote startup, all nonterminal synchronous OpenAI Generations enter cancellation before Worker sessions are accepted, so lost client connections cannot leave invisible work running; durable Native Generations retain their leases and resume through Worker reconnection.
