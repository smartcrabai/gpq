# Collapse Task into Generation

The native queue uses Generations directly and Attempts reference them; no Task entity, table, identifier, or external API exists until a real multi-step or DAG requirement appears. PostgreSQL stores tenant and credential, Worker and Device Pool, Model alias/version, Generation, Attempt, Artifact, idempotency, and Generation-event records. Typed Image/Video Generation parameters, Model Manifests, and Shape Limits are represented as JSONB; identifiers, timestamps, and lifecycle states use UUIDv7, `TIMESTAMPTZ`, and checked text values.
