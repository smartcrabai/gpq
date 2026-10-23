# Separate migration from serving

`gpq remote migrate` runs embedded SQLx migrations with the schema-owner credential, while `gpq remote serve` receives only the forced-RLS application credential and refuses to start against an unexpected migration version. Native cutover migration 0008 requires fresh/empty feature data and refuses legacy rows rather than deleting or converting them. Preserve and back up the legacy database, then perform cutover with a fresh database; no history-compatible conversion is promised. Schema changes remain forward-only.
