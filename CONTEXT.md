# GPU Generation Queue

A multi-tenant system that coordinates tenant-owned native image and video generation Workers through PostgreSQL.

## Language

**Tenant**:
The isolated customer boundary that owns Generations and Workers. A Worker belongs to exactly one Tenant and never executes another Tenant's work.
_Avoid_: Account, customer

**Tenant Master Key**:
The tenant-wide administrative credential used by public APIs, key rotation, and one-time Worker enrollment. It is not stored on an enrolled Worker.
_Avoid_: Worker Credential, user key

**Generation**:
A Tenant's typed request for one generated image or video. A Generation settles on one Accepted Result even when execution requires multiple Attempts.
_Avoid_: Request, job

**Attempt**:
One execution of a Generation by a Worker. Retries create new Attempts and may consume additional GPU time or produce different output.
_Avoid_: Retry

**Worker**:
A tenant-scoped local service that executes Generations using the native inference runtime on attached Device Pools. A Tenant may operate multiple Workers.
_Avoid_: Consumer, agent

**Worker Credential**:
A revocable machine credential issued to one Worker during enrollment. It authorizes only Worker protocol operations for that Worker's Tenant.
_Avoid_: Tenant Master Key, user key

**Device Pool**:
A non-overlapping set of one or more GPUs controlled as one exclusive resource by a Worker. Each Pool owns one private native inference child.
_Avoid_: GPU queue

**Active Runtime**:
The native inference child process occupying a Device Pool. It is spawned and supervised by `gpq worker` from the same `gpq` executable; the configured accelerator selects the native device implementation.
_Avoid_: Worker, Device Pool

**Execution Slot**:
One unit of concurrent execution capacity advertised by a Device Pool. The current per-Pool native inference child processes one Generation at a time.
_Avoid_: GPU, worker thread

**Artifact**:
A large binary input or output associated with a Generation. Remote-local input bytes are stored in PostgreSQL until the Generation's terminal transition; output Artifacts are consumed once or expire independently.
_Avoid_: Blob, permanent file

**Artifact Manifest**:
The immutable size, SHA-256 digest, media kind, and MIME type describing an Artifact before transfer or result commitment.
_Avoid_: File metadata

**Modality**:
The typed generation kind, either Image or Video. The model family determines modality and request parameters must match it.
_Avoid_: output format

**Model**:
A logical model alias requested by a Generation. Admission resolves the alias to one immutable Model Version.
_Avoid_: Model file, checkpoint

**Model Manifest**:
The immutable family, required asset-role hashes, default LoRA selections, and sampling preset for one configured native model. Its canonical SHA-256 hash identifies that exact configuration; filesystem paths are local to a Worker and are not part of the manifest.
_Avoid_: Worker model path

**Model Version**:
Exact model material identified by its manifest hash and advertised by capable Workers. Every Attempt of a Generation uses the same Model Version.
_Avoid_: Current model, local filename

**Accepted Result**:
The first successful Attempt result committed for a Generation. Later duplicate results do not replace it.
_Avoid_: Latest result, final attempt
