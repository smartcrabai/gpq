# Use leased at-least-once execution

Generations use PostgreSQL-backed leased at-least-once delivery because Remote cannot prove whether a disconnected Worker began or completed native inference. Each retry creates a distinct Attempt; the first result committed against a live lease and an unaccepted Generation becomes the Accepted Result, and later or stale results cannot replace it. Automatic retries are limited to Worker loss, lease expiry, inference-process crashes, transfer failures, and candidate-specific runtime OOM, with at most three Attempts per Generation; invalid input, unsupported capability, unavailable models, timeouts, cancellation, and internal failures are not retried. Worker retry hints cannot widen Remote's policy.

Generation states are `Queued`, `Running`, `Cancelling`, `Succeeded`, `Failed`, `Cancelled`, and `Expired`; Attempt states are `Leased`, `Running`, `Succeeded`, `Failed`, `Cancelled`, and `LeaseExpired`. Transitions are monotonic, retries create new Attempts, and output delivery does not delay a Generation's `Succeeded` transition.

Known tenant, manifest, LoRA, modality, and configured shape mismatches are excluded before an Attempt. Runtime OOM fails the Attempt and excludes that Pool only for the current Generation; it does not persist negative capability or treat estimated memory as a hard gate. The Generation fails for OOM when no eligible candidate remains or the retry limit is reached.

Result acceptance is a PostgreSQL compare-and-set transaction that validates the live lease, a nonterminal Generation, and the absence of a prior Accepted Result before atomically committing the Attempt and Generation as succeeded. Stale or duplicate results cannot replace the accepted result; Artifact delivery follows the configured output placement and does not delay Generation success.

Queued cancellation terminates immediately; running cancellation enters `Cancelling` until Worker acknowledgement. Cancellation acknowledgement and result commitment race through terminal compare-and-set, so whichever commits first wins and the loser observes the final state. Repeated cancellation is idempotent.

Execution timeout defaults to two hours for Image and 24 hours for Video. A Model Version may set an execution limit, a request may only shorten it, and the Tenant ceiling remains authoritative. Timeout starts when the Attempt enters `Running`, is not retried, and requests cancellation with native child-process termination as the fallback when cooperative cancellation does not complete.

Workers report normalized failure kinds (`InvalidInput`, `UnsupportedCapability`, `ModelUnavailable`, `OutOfMemory`, `BackendCrashed`, `ExecutionTimedOut`, `Cancelled`, `TransferFailed`, or `Internal`); Remote records `WorkerLost` and `LeaseExpired` as well. Remote alone applies the retry policy, while raw errors remain diagnostic data.

