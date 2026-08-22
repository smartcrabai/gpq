# Defer database portability

The current system targets PostgreSQL directly and may use row-level security, `SKIP LOCKED`, and `LISTEN/NOTIFY` without compatibility abstractions. A production move to Aurora DSQL or CockroachDB is a separate future migration with its own queue and isolation redesign; current backup and disaster-recovery requirements are likewise out of scope.
