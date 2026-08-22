# Collapse Task into Generation

The initial model queues Generations directly and Attempts reference them; no Task entity, table, identifier, or external API exists until a real multi-step or DAG requirement appears. PostgreSQL stores the remaining tenant, credential, Worker and Device Pool, model and workflow catalog, Generation, Attempt, Artifact, idempotency, and Generation event records using UUIDv7, `TIMESTAMPTZ`, text checks for state, and JSONB only for backend-shaped data.
