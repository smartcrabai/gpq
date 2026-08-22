# GPU Generation Queue

A multi-tenant system that coordinates tenant-owned GPU workers and AI generation requests while prioritizing GPU utilization.

## Language

**Tenant**:
The isolated customer boundary that owns Generations and Workers. A Worker belongs to exactly one Tenant and never executes another Tenant's work.
_Avoid_: Account, customer

**Tenant Master Key**:
The tenant-wide administrative credential used by public APIs, key rotation, and Worker enrollment. It is not stored on enrolled Workers.
_Avoid_: API key, worker key

**Generation**:
A tenant's request for one AI-generated result. A Generation settles on one Accepted Result even when execution requires multiple Attempts.
_Avoid_: Request, job

**Attempt**:
One execution of a Generation by a Worker. Retries create new Attempts and may consume additional GPU time or produce different output.
_Avoid_: Retry

**Worker**:
A tenant-scoped local service that executes Generations on attached generation backends. A Tenant may operate multiple Workers.
_Avoid_: Consumer, agent

**Worker Credential**:
A revocable machine credential issued to one Worker during enrollment. It authorizes only Worker protocol operations for that Worker's Tenant.
_Avoid_: Tenant Master Key, user key

**Device Pool**:
A non-overlapping set of one or more GPUs controlled as one exclusive resource by a Worker. It hosts at most one Active Runtime at a time.
_Avoid_: GPU queue

**Active Runtime**:
The currently loaded llama.cpp or ComfyUI runtime occupying a Device Pool. Switching backend kind replaces the Active Runtime rather than colocating both.
_Avoid_: Backend installation, Worker

**Execution Slot**:
One concurrent execution permit exposed by an Active Runtime. llama.cpp may expose several Slots through continuous batching; ComfyUI normally exposes one.
_Avoid_: GPU, worker thread

**Artifact**:
A transient large binary input or output associated with a Generation. Input Artifacts live through the Generation's terminal transition; output Artifacts live through one delivery attempt or their expiry.
_Avoid_: Blob, permanent file

**Artifact Manifest**:
The immutable size, SHA-256 digest, media kind, and MIME type describing an Artifact before transfer or result commitment.
_Avoid_: File metadata

**Model**:
A logical AI model alias requested by a Generation. Admission resolves it to one immutable Model Version.
_Avoid_: Model file, checkpoint

**Model Version**:
Exact model material identified by a content hash and advertised by capable Workers. Every Attempt of a Generation uses the same Model Version.
_Avoid_: Current model, local filename

**Workflow**:
A logical ComfyUI execution graph alias used by image, video, or music Generations. Its backend-specific parameters are not part of a universal modality schema.
_Avoid_: Pipeline, template

**Workflow Version**:
An immutable Workflow graph and output contract identified by a content hash and fixed when a Generation is admitted.
_Avoid_: Current workflow

**Accepted Result**:
The first successful Attempt result committed for a Generation. Later duplicate results do not replace it.
_Avoid_: Latest result, final attempt
