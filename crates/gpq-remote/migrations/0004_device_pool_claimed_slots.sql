-- Device Pool free-Slot accounting becomes claim-side authoritative.
--
-- `device_pools.free_slots` used to be an absolute value a Worker's
-- `CapabilityReport` overwrote wholesale (`db::workers::upsert_pools`'s
-- `ON CONFLICT ... SET free_slots = excluded.free_slots`), computed by the
-- Worker from its own `busy` flags with no knowledge of Remote's
-- `claim_slot`/`release_slot` counters. Remote decrements a Slot the moment
-- it schedules a lease, before the Worker has even received it, so a
-- capability report arriving in that window reset `free_slots` and let the
-- scheduler double-book a single-Slot Pool (ADR 0002, ADR 0005).
--
-- `claimed_slots` is now Remote's own counter, never written by a
-- capability report; `free_slots` becomes a column generated from it, so no
-- caller can ever write a stale absolute back into it. A Worker that
-- disappears mid-lease leaks its claim only until the lease-expiry sweep
-- (`expiry.rs`) fails the stranded Attempt and calls `release_slot`.

ALTER TABLE device_pools
    ADD COLUMN claimed_slots integer NOT NULL DEFAULT 0 CHECK (claimed_slots >= 0);

ALTER TABLE device_pools
    ADD CONSTRAINT device_pools_claimed_within_total CHECK (claimed_slots <= total_slots);

ALTER TABLE device_pools DROP COLUMN free_slots;

ALTER TABLE device_pools
    ADD COLUMN free_slots integer GENERATED ALWAYS AS (total_slots - claimed_slots) STORED;
