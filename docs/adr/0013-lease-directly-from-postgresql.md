# Lease Generations directly from PostgreSQL

PostgreSQL rows remain the only queue truth: a tenant-scoped `FOR UPDATE SKIP LOCKED` transaction selects work and creates its Attempt and lease atomically using database time. `LISTEN/NOTIFY` only wakes waiting sessions, with a one-second fallback query because notifications are not durable; no in-memory queue or external message broker is introduced.
