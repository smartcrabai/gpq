# Separate migration from serving

`gpq-remote migrate` runs embedded SQLx migrations with the schema-owner credential, while `gpq-remote serve` receives only the forced-RLS application credential and refuses to start against an unexpected migration version. This adds one deployment step but keeps everyday network-facing code unable to alter schema or bypass tenant isolation; schema changes use forward fixes rather than rollback migrations.
