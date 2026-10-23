# Graph Report - gpq  (2026-10-21)

## Corpus Check
- 122 files · ~139,656 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 15 file(s) not represented in the graph (top: (none) 5, .proto 5, .cmake 2)

## Summary
- 3002 nodes · 7458 edges · 145 communities (121 shown, 24 thin omitted)
- Extraction: 97% EXTRACTED · 3% INFERRED · 0% AMBIGUOUS · INFERRED: 209 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `ab6df193`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- e2e.rs
- ArtifactService
- workers.rs
- Harness
- diagnose.rs
- generations.rs
- .watch_generation
- NativeOutput
- SessionCtx
- tenants.rs
- AppState
- postgres.rs
- gpq-inference/src/lib.rs
- ManagedProcess
- pool.rs
- executor.rs
- infer.rs
- attempts.rs
- GenerationEvent
- objectstore.rs
- .register_workflow_version
- e2e_support/mod.rs
- transfer.rs
- gpq-remote/src/session.rs
- native/mod.rs
- WorkerRegistry
- main.ts
- admission.rs
- Lease Generations directly from PostgreSQL
- try_handle_attempt_failure
- Test workflow
- native_artifact_safety.rs
- media.cpp
- service.rs
- src/generation.rs
- gpq-worker/src/artifacts.rs
- TenantId
- gpq-worker/src/config.rs
- db/catalog.rs
- IncapableReason
- auth.rs
- scheduler.rs
- Db
- CredentialStore
- image_request
- backend/mod.rs
- native/generation.rs
- gpq-remote/src/config.rs
- write_image_outputs
- src/models.rs
- gpq-remote coordinator
- schedule.rs
- process.rs
- expired_attempt_ids
- super
- db/artifacts.rs
- container.rs
- bridge.cpp
- ConnectError
- FailureKind
- tenant_console.rs
- Response
- plan_assignments
- ModelManifest
- dispatch_worker_message
- GenerationApi
- capability.rs
- gpq-worker/src/lib.rs
- lock_state
- Hasher
- db/mod.rs
- ModelManifestError
- context
- http.rs
- gpq_native_generate
- drain_worker_chunks
- Active Runtime
- gpq-domain
- super
- package.json
- Artifact
- Model
- Workflow
- Artifact download endpoint
- remote_session_start.sh
- main.rs
- duration
- DeliveryChunk
- db.rs
- Command
- connect_and_serve
- AbortOnDrop
- ArtifactState
- objectstore_support/mod.rs
- ProcessIdentity
- src/cli.rs
- SchedulerHandle
- gpq-remote/src/artifacts.rs
- AGENTS.md
- TenantCommand
- accept_lease
- compilerOptions
- ManifestMismatch
- generation_row_to_proto
- LoraSet
- e2e_support/cli.rs
- JobHandle
- gpq-worker/src/session.rs
- handle_remote_message
- ImageArray
- ArtifactId
- Ownership
- Trust Remote and enforce database tenant isolation
- record_accepted_result
- ProgressRegistrationData
- wait_until
- terminate_group
- NativeStatus
- gpq-domain/src/lib.rs
- classify_result_outcome
- NativeEvent
- Position
- AudioOwner
- PngFileSink
- HarnessEntry
- Separate public and Worker transports
- ChunkDrainOutcome
- DeliveryRequestOutcome
- install-spirv-headers.sh
- shared Generation envelope
- 0012-pin-model-versions-at-admission.md
- 0015-use-six-rust-crates.md
- native_exclusions.rs
- .submit
- ContainerLease
- DownloadOutcome
- .ensure_runtime_and_acquire_slot
- ArtifactDirection
- .from_row
- SendOutcome

## God Nodes (most connected - your core abstractions)
1. `ContentHash` - 111 edges
2. `AppState` - 92 edges
3. `ModelManifest` - 50 edges
4. `ArtifactManifest` - 47 edges
5. `FailureKind` - 42 edges
6. `Harness` - 37 edges
7. `SyntheticWorker` - 35 edges
8. `ArtifactRow` - 34 edges
9. `harness()` - 34 edges
10. `ModelFamily` - 33 edges

## Surprising Connections (you probably didn't know these)
- `Use native typed APIs` --references--> `ImageParameters`  [INFERRED]
  docs/adr/0006-use-native-typed-apis.md → examples/native-image-video/main.ts
- `Use native typed APIs` --references--> `VideoParameters`  [INFERRED]
  docs/adr/0006-use-native-typed-apis.md → examples/native-image-video/main.ts
- `revoked_worker_credential_blocks_reconnection_and_scheduling()` --calls--> `image_request()`  [INFERRED]
  crates/gpq/tests/lifecycle.rs → crates/gpq/tests/e2e_support/mod.rs
- `session_handshake_rejects_incompatible_protocol_major()` --calls--> `harness()`  [INFERRED]
  crates/gpq/tests/lifecycle.rs → crates/gpq/tests/objectstore.rs
- `harness_teardown_waits_for_active_uses()` --calls--> `harness()`  [INFERRED]
  crates/gpq/tests/lifecycle.rs → crates/gpq/tests/objectstore.rs

## Import Cycles
- 1-file cycle: `crates/gpq-domain/src/state.rs -> crates/gpq-domain/src/state.rs`
- 1-file cycle: `crates/gpq-worker/src/executor.rs -> crates/gpq-worker/src/executor.rs`
- 1-file cycle: `crates/gpq-worker/src/pool.rs -> crates/gpq-worker/src/pool.rs`
- 1-file cycle: `crates/gpq-worker/src/infer.rs -> crates/gpq-worker/src/infer.rs`
- 1-file cycle: `crates/gpq-worker/src/models.rs -> crates/gpq-worker/src/models.rs`
- 1-file cycle: `crates/gpq-worker/src/config.rs -> crates/gpq-worker/src/config.rs`
- 1-file cycle: `crates/gpq-remote/src/db/mod.rs -> crates/gpq-remote/src/db/mod.rs`
- 1-file cycle: `crates/gpq-remote/src/db/events.rs -> crates/gpq-remote/src/db/events.rs`
- 1-file cycle: `crates/gpq-worker/src/credential.rs -> crates/gpq-worker/src/credential.rs`
- 1-file cycle: `crates/gpq-worker/src/diagnose.rs -> crates/gpq-worker/src/diagnose.rs`
- 1-file cycle: `crates/gpq-worker/src/lib.rs -> crates/gpq-worker/src/lib.rs`
- 1-file cycle: `crates/gpq-worker/src/process.rs -> crates/gpq-worker/src/process.rs`
- 1-file cycle: `crates/gpq-worker/src/service.rs -> crates/gpq-worker/src/service.rs`
- 2-file cycle: `crates/gpq-remote/src/scheduler.rs -> crates/gpq-remote/src/state.rs -> crates/gpq-remote/src/scheduler.rs`
- 2-file cycle: `crates/gpq-remote/src/artifacts.rs -> crates/gpq-remote/src/state.rs -> crates/gpq-remote/src/artifacts.rs`
- 2-file cycle: `crates/gpq/tests/e2e_support/mod.rs -> crates/gpq/tests/e2e_support/synthetic.rs -> crates/gpq/tests/e2e_support/mod.rs`

## Hyperedges (group relationships)
- **CI quality gate** — github_workflows_test_protobuf_lint, github_workflows_test_cargo_check, github_workflows_test_unit_tests, github_workflows_test_clippy [EXTRACTED 1.00]
- **Tenant generation worker execution flow** — context_tenant, context_generation, context_attempt, context_worker, context_accepted_result [EXTRACTED 1.00]
- **Generation execution and delivery** — docs_adr_0002_prioritize_gpu_utilization_prioritize_gpu_utilization, docs_adr_0003_use_leased_at_least_once_execution_use_leased_at_least_once_execution, docs_adr_0007_unify_generation_lifecycle_only_unify_the_generation_lifecycle_only, docs_adr_0008_keep_records_but_expire_artifacts_keep_records_but_expire_artifacts [INFERRED 0.85]

## Communities (145 total, 24 thin omitted)

### Community 0 - "e2e.rs"
Cohesion: 0.14
Nodes (29): alias_without_online_worker(), backend_failure_exhausts_retries_and_fails_generation(), catalog_requires_master_key_and_lists_aliases(), count_nonterminal(), create_input(), cross_tenant_isolation_over_the_api(), delete_model_alias_removes_listing_without_mutating_pinned_generations(), get_tenant_settings_returns_migration_defaults() (+21 more)

### Community 2 - "workers.rs"
Cohesion: 0.15
Nodes (25): build_slot_capability(), builds_slot_capability_from_the_pool_row(), claim_slot(), clear_session(), decode_hash(), empty_shape_limits(), enroll(), free_slots_clamps_negative_database_values_to_zero() (+17 more)

### Community 3 - "Harness"
Cohesion: 0.13
Nodes (5): AdminCli, Harness, spawn_worker(), TenantFixture, WorkerSpawn

### Community 4 - "diagnose.rs"
Cohesion: 0.20
Nodes (16): WorkerConfig, check_accelerator(), check_credential(), check_executable(), check_managed_process(), check_models(), check_remote(), check_state_dir_writable() (+8 more)

### Community 5 - "generations.rs"
Cohesion: 0.06
Nodes (54): i16, Priority, .DEFAULT, .MAX, .MIN, PriorityOutOfRange, u8, ContentHashError (+46 more)

### Community 6 - ".watch_generation"
Cohesion: 0.16
Nodes (9): domain_event_to_proto(), progress_from_json(), progress_json_round_trips(), replay_events(), replay_events_drops_a_malformed_payload(), replay_events_skips_attempt_created_rows(), replay_events_stamps_emitted_at_from_the_persisted_row_not_now(), sample_row() (+1 more)

### Community 7 - "NativeOutput"
Cohesion: 0.16
Nodes (16): NativeOutput, arm_release_grace(), execution_timeout_duration(), fetch_inputs(), handle_event(), internal(), invalid(), manifest_for_output() (+8 more)

### Community 8 - "SessionCtx"
Cohesion: 0.31
Nodes (5): capability_report_message(), heartbeat_message(), reject_lease(), SessionCtx, wrap_worker_message()

### Community 9 - "tenants.rs"
Cohesion: 0.07
Nodes (41): overdue_is_inclusive_of_the_boundary(), TenantSettings, .DEFAULT_MAXIMUM_QUEUE_AGE, create_tenant(), decode_error(), delete_tenant(), encode_error(), insert_master_key() (+33 more)

### Community 10 - "AppState"
Cohesion: 0.12
Nodes (23): cancel_request_carries_the_attempt_id_and_reason(), cancel_request_message(), enforce_execution_deadlines(), expire_artifacts(), expire_leases(), expire_starved_queue(), input_object_key(), object_store_input_missing_its_key_takes_no_action() (+15 more)

### Community 11 - "postgres.rs"
Cohesion: 0.07
Nodes (51): FixtureUse<T>, accept_result_rejects_a_result_for_a_terminal_generation(), accept_result_rejects_a_result_under_an_expired_lease(), accept_result_rejects_a_second_result_once_accepted(), accept_result_settles_the_first_result_and_releases_the_slot(), active_tenants_excludes_soft_deleted_tenants(), artifact_lifecycle_available_to_delivering_to_consumed(), artifact_output_expires_one_hour_after_completion() (+43 more)

### Community 12 - "gpq-inference/src/lib.rs"
Cohesion: 0.08
Nodes (60): SamplingPreset, backend_assignment(), BackendDevice, cancel_current(), generate(), gpq_native_cancel_current(), gpq_native_free_string(), gpq_native_generate() (+52 more)

### Community 13 - "ManagedProcess"
Cohesion: 0.16
Nodes (5): assert_fixture_descendant_gone(), managed_process_terminates_its_group_without_a_path_kill_helper(), ManagedProcess, ProcessGuard, run_process_tree_fixture()

### Community 14 - "pool.rs"
Cohesion: 0.12
Nodes (13): dropping_slot_lease_does_not_reopen_an_unready_pool(), IDLE_UNLOAD_AFTER, ModelAdvertisementData, PoolAdvertisementData, PROBE_RETRY_INTERVAL, PROCESS_IDENTITY_FILE, resolved_model(), runtime_options() (+5 more)

### Community 15 - "executor.rs"
Cohesion: 0.07
Nodes (20): decode_lease(), ExecutionOutcome, generation_parameters_from_proto(), lora_from_proto(), manifest_from_proto(), manifest_to_proto(), media_kind_from_proto(), media_kind_to_proto() (+12 more)

### Community 16 - "infer.rs"
Cohesion: 0.07
Nodes (48): ActiveJob, ActiveJobGuard, Args, assert_idle(), blocking_work_keeps_slot_after_http_wait_is_dropped(), CancelRequest, constant_time_eq(), drain_events() (+40 more)

### Community 17 - "attempts.rs"
Cohesion: 0.11
Nodes (34): RetryDecision, Fail, Requeue, HEARTBEAT_INTERVAL, lease_expires_forty_five_seconds_after_now(), lease_expiry_from(), LEASE_TTL, AttemptState (+26 more)

### Community 18 - "GenerationEvent"
Cohesion: 0.07
Nodes (32): GenerationState, append(), append_attempt_created(), EventKind, AttemptCreated, Progress, StateChanged, EventRow (+24 more)

### Community 19 - "objectstore.rs"
Cohesion: 0.12
Nodes (35): block_on(), acquire(), build_with_object_store(), cleanup_with_object_store(), cleanup_without_object_store(), clear_registry(), create_and_upload_input(), create_input_artifact_presigns_upload_and_is_downloadable() (+27 more)

### Community 20 - ".register_workflow_version"
Cohesion: 0.12
Nodes (11): catalog_error(), CatalogApi, internal(), invalid(), model_alias_to_proto(), model_asset_role_to_proto(), model_available(), model_family_to_proto() (+3 more)

### Community 21 - "e2e_support/mod.rs"
Cohesion: 0.10
Nodes (14): database_url_for(), database_url_with_credentials(), free_port(), HARNESS_REGISTRY, HarnessOptions, HarnessRegistry, POLL_INTERVAL, random_hex() (+6 more)

### Community 22 - "transfer.rs"
Cohesion: 0.06
Nodes (20): ChunkReceipt, Delivered, NoDownloader, DeliveryValidation, Accepted, Rejected, Retryable, domain_manifest() (+12 more)

### Community 23 - "gpq-remote/src/session.rs"
Cohesion: 0.05
Nodes (15): advertised_model_is_kept_when_its_manifest_hashes_to_its_version(), advertised_model_whose_manifest_does_not_hash_to_its_version_is_dropped(), domain_asset_role(), domain_failure_kind(), domain_model_family(), domain_placement(), model_manifest_from_proto(), pool_upsert_from_proto() (+7 more)

### Community 24 - "native/mod.rs"
Cohesion: 0.07
Nodes (24): EnrollmentApi, internal(), internal_anyhow(), artifact_placement_from_proto(), artifact_placement_round_trips_through_proto(), artifact_placement_to_proto(), artifact_state_to_proto(), authenticate() (+16 more)

### Community 25 - "WorkerRegistry"
Cohesion: 0.17
Nodes (8): online_workers_is_scoped_to_its_tenant(), registered_worker_is_online_and_reachable(), send_reports_backpressure_without_marking_the_worker_offline(), SessionEntry, SessionGuard, stale_guard_does_not_evict_a_newer_session(), worker_message(), WorkerRegistry

### Community 26 - "main.ts"
Cohesion: 0.06
Nodes (35): Use native typed APIs, activeStates, ArtifactRef, awaitGeneration(), baseUrl, cancelGeneration(), checkInterrupted(), createInputArtifact() (+27 more)

### Community 27 - "admission.rs"
Cohesion: 0.12
Nodes (29): ArtifactPlacement, AdmissionError, CapacityExceeded, Internal, InvalidInput, ObjectStoreUnavailable, UnknownAlias, AdmissionRequest (+21 more)

### Community 28 - "Lease Generations directly from PostgreSQL"
Cohesion: 0.11
Nodes (9): cache-aware GPU scheduling, leased at-least-once delivery, device pools and managed runtimes, ephemeral Artifact lifecycle, PostgreSQL durable state, PostgreSQL queue truth, PostgreSQL-specific queue semantics, Generation as queue entity (+1 more)

### Community 29 - "try_handle_attempt_failure"
Cohesion: 0.40
Nodes (3): dispatch_attempt_failure(), log_on_err(), try_handle_attempt_failure()

### Community 30 - "Test workflow"
Cohesion: 0.07
Nodes (36): Buf breaking check, Buf configuration, Buf lint, proto module, STANDARD lint rules, Build and release job, Docker Buildx, Docker image build and push (+28 more)

### Community 31 - "native_artifact_safety.rs"
Cohesion: 0.06
Nodes (37): capability_report(), enroll(), open_session(), proto_family(), proto_manifest(), proto_role(), shared_transport(), SyntheticWorker (+29 more)

### Community 32 - "media.cpp"
Cohesion: 0.07
Nodes (11): ~CheckedMkvWriter(), close(), open(), resample_interleaved_audio(), set_error(), TemporaryOutput, committed_, destination_ (+3 more)

### Community 33 - "service.rs"
Cohesion: 0.09
Nodes (34): emit_library(), emit_link_manifest(), emit_system_library(), feature(), framework_name(), library_stem(), main(), PROTOS (+26 more)

### Community 34 - "src/generation.rs"
Cohesion: 0.07
Nodes (33): family_validation_rejects_modality_mismatch(), Image, image_parameters(), image_request_bounds_prompts_and_rejects_invalid_steps_and_lora_scales(), image_request_validates_shape_and_sampling_and_bounds_references(), ImageParameters, nonempty_sigma_schedule_requires_at_least_two_finite_values(), ParameterValidationError (+25 more)

### Community 35 - "gpq-worker/src/artifacts.rs"
Cohesion: 0.17
Nodes (20): ArtifactHandle, DATA_FILE, delete_is_idempotent(), expire_removes_only_artifacts_past_ttl(), LocalArtifactStore, manifest(), MANIFEST_FILE, open_for_read_rejects_an_offset_past_the_manifest() (+12 more)

### Community 36 - "TenantId"
Cohesion: 0.18
Nodes (15): generation_snapshot(), GenerationSnapshot, handle_capability_report(), load_tenant_settings(), lock_current_worker_credential(), output_placement_worker(), parse_uuid_id(), publish_state_event() (+7 more)

### Community 37 - "gpq-worker/src/config.rs"
Cohesion: 0.08
Nodes (26): check_device_overlap(), ConfiguredLora, cuda_pools_are_partitioned(), DEVICE_SELECTOR_ENV_KEYS, duration_secs(), ensure_asset_path(), ensure_state_dir(), ModelConfig (+18 more)

### Community 38 - "db/catalog.rs"
Cohesion: 0.12
Nodes (23): ContentHash, ExecutionLimits, CatalogError, Database, UnknownVersion, decode_error(), decode_limits(), decode_manifest() (+15 more)

### Community 39 - "IncapableReason"
Cohesion: 0.21
Nodes (10): IncapableReason, ImagePixelLimit, InvalidShape, MissingLora, MissingModel, ModelModalityMismatch, StepLimit, TenantMismatch (+2 more)

### Community 40 - "auth.rs"
Cohesion: 0.12
Nodes (11): bearer_token(), bearer_token_parses_case_insensitively(), bearer_token_parses_the_standard_scheme(), bearer_token_rejects_other_schemes(), different_secrets_hash_differently(), generate_secret(), generated_secrets_carry_their_prefix_and_are_unique(), hasher() (+3 more)

### Community 41 - "scheduler.rs"
Cohesion: 0.11
Nodes (18): MediaKind, build_lease_assignment(), CANDIDATE_FETCH_LIMIT, duration_from_micros_round_trips(), duration_from_negative_micros_falls_back_to_zero(), FALLBACK_POLL_INTERVAL, leased_output_key(), proto_artifact_manifest() (+10 more)

### Community 43 - "CredentialStore"
Cohesion: 0.14
Nodes (13): CredentialStorage, Platform, accepts_owner_only_file(), CredentialStore, current_uid(), explicit_file_storage_round_trips_with_private_permissions(), load_unix_file_credential(), missing_file_is_none() (+5 more)

### Community 44 - "image_request"
Cohesion: 0.24
Nodes (10): image_request(), duplicate_accepted_result_preserves_worker_local_downloadability(), expired_heartbeat_is_not_renewed(), foreign_worker_controls_and_oversized_result_leave_the_owned_attempt_unchanged(), lease(), pool_free_slots(), revoked_worker_cannot_commit_result_without_a_prior_heartbeat(), unsolicited_cancel_ack_preserves_running_generation_and_slot() (+2 more)

### Community 45 - "backend/mod.rs"
Cohesion: 0.12
Nodes (15): Backend, BackendCapabilities, BackendError, build(), ExecutionEvent, Output, Progress, ExecutionRequest (+7 more)

### Community 46 - "native/generation.rs"
Cohesion: 0.08
Nodes (5): DEFAULT_PAGE_SIZE, IDEMPOTENCY_KEY_HEADER, MAX_IDEMPOTENCY_KEY_LEN, MAX_PAGE_SIZE, ProgressJson

### Community 47 - "gpq-remote/src/config.rs"
Cohesion: 0.15
Nodes (10): credential_key_parses_valid_hex(), DEFAULT_BIND_ADDR, DEFAULT_PRESIGN_TTL_SECS, object_store_absent_is_none(), object_store_default_presign_ttl_is_fifteen_minutes(), object_store_full_configuration_resolves(), ObjectStoreConfig, parse_credential_key() (+2 more)

### Community 48 - "write_image_outputs"
Cohesion: 0.14
Nodes (20): ActiveContext, context, commit_output(), decode_image(), first_lora_warning_message(), GeneratedFiles, committed, paths (+12 more)

### Community 49 - "src/models.rs"
Cohesion: 0.13
Nodes (26): CACHE_FILE_NAME, cache_hits_for_an_unchanged_file(), cache_key(), cache_misses_when_content_and_size_change(), CacheEntry, CacheKey, collect_model_files(), collect_model_files_with_cancel() (+18 more)

### Community 50 - "gpq-remote coordinator"
Cohesion: 0.13
Nodes (18): Accepted Result, Attempt, Generation, Tenant, Tenant Master Key, Worker, Worker Credential, ComfyUI (+10 more)

### Community 51 - "schedule.rs"
Cohesion: 0.19
Nodes (16): age_breaks_ties_within_equal_priority(), batches_share_the_pinned_version_and_respect_capacity(), Candidate, Fixture, incompatible_work_is_excluded(), manifest(), model_hash(), no_free_slot_means_no_work() (+8 more)

### Community 52 - "process.rs"
Cohesion: 0.09
Nodes (5): command_line_corroboration_checks_the_native_worker_executable(), filetime_to_u64(), IDENTITY_TEMP_FILE_COUNTER, ownership_table_requires_every_field_to_match(), sample_identity()

### Community 53 - "expired_attempt_ids"
Cohesion: 0.29
Nodes (7): cancel_all_attempts(), cancel_attempt(), cancel_expired_leases(), expired_attempt_ids(), live_attempt_ids(), LiveAttempt, renew_leases()

### Community 54 - "super"
Cohesion: 0.09
Nodes (5): digest_matches_known_vector(), round_trips_through_hex(), typed_id!, PROTOCOL_MAJOR, PROTOCOL_MINOR

### Community 55 - "db/artifacts.rs"
Cohesion: 0.22
Nodes (18): ArtifactRow, attempt_input(), begin_delivery(), commit_offset(), create_input(), create_remote_local_input(), delete_inputs_for_generation(), delivery_expired() (+10 more)

### Community 56 - "container.rs"
Cohesion: 0.15
Nodes (6): CLEANUP_RUNTIME, CONTAINER, ContainerState, LeaseTracker, REAP_REQUESTED, SharedContainer

### Community 57 - "bridge.cpp"
Cohesion: 0.10
Nodes (6): ContextDeleter, diffusion_backend_from_assignment(), equals_case_insensitive(), has_vulkan_prefix(), LoraApplicationPhase, trim_view()

### Community 58 - "ConnectError"
Cohesion: 0.18
Nodes (7): admission_error(), artifact_row_to_proto(), image_parameters_from_proto(), internal(), optional_artifact_id(), parse_artifact_id(), video_parameters_from_proto()

### Community 59 - "FailureKind"
Cohesion: 0.11
Nodes (13): FailureKind, parse_failure_kind(), File, DirGuard, download_input(), execute(), ExecutionContext, failure_kind_to_proto() (+5 more)

### Community 60 - "tenant_console.rs"
Cohesion: 0.29
Nodes (6): console_route_serves_the_browser_client_with_security_headers(), CONTENT_SECURITY_POLICY, INDEX, index(), PERMISSIONS_POLICY, router()

### Community 61 - "Response"
Cohesion: 0.31
Nodes (7): Response, deliver_worker_local(), download_artifact(), download_for_tenant(), redirect_to_object_store(), upload_artifact(), upload_for_tenant()

### Community 62 - "plan_assignments"
Cohesion: 0.22
Nodes (15): assign(), eligible_pool_is_bounded_by_its_advertised_free_slots(), known_tenants(), oom_excluded_pool_does_not_receive_generation_again(), plan_assignments(), pool_whose_worker_has_no_live_session_is_skipped(), pool_with_no_free_slots_is_skipped(), sample_candidate() (+7 more)

### Community 63 - "ModelManifest"
Cohesion: 0.11
Nodes (16): ExecutionTarget, target_pins_version_and_derives_modality_from_family(), Modality, DAY, HOUR, LoraSelection, manifest_hash_is_stable_and_lora_order_is_identity(), manifests_validate_required_assets_sampling_consistency_and_finite_numbers() (+8 more)

### Community 64 - "dispatch_worker_message"
Cohesion: 0.17
Nodes (14): authenticate_session(), AuthenticatedWorker, discard_worker_outputs(), dispatch_attempt_result(), dispatch_worker_message(), handle_heartbeat(), internal(), not_renewed_attempts_each_get_a_cancel_request() (+6 more)

### Community 66 - "capability.rs"
Cohesion: 0.19
Nodes (21): add_model(), any_candidate_remains(), effective_schedule_steps_drive_image_admission_limits(), explicit_steps_must_match_selected_schedule_and_empty_sigmas_fall_back(), GenerationShape, image_loras_must_be_separately_advertised(), image_parameters(), image_shape_and_sampling_limits_exclude_unsupported_requests() (+13 more)

### Community 67 - "gpq-worker/src/lib.rs"
Cohesion: 0.20
Nodes (17): combine_uninstall_results(), combine_uninstall_results_ok_when_both_succeed(), combine_uninstall_results_reports_both_failures_when_neither_step_succeeds(), combine_uninstall_results_surfaces_credential_failure_alone(), combine_uninstall_results_surfaces_service_failure_alone(), diagnose_cmd(), dispatch(), enroll() (+9 more)

### Community 68 - "lock_state"
Cohesion: 0.18
Nodes (9): lock_state(), mark_unready(), PoolEntry, PoolSupervisor, probe_pool_process(), start_pool_process(), terminate_managed_process(), TerminatingProcessGuard (+1 more)

### Community 70 - "db/mod.rs"
Cohesion: 0.19
Nodes (8): interval_days_and_microseconds_combine(), interval_days_are_folded_into_a_fixed_day(), interval_months_fold_into_thirty_day_periods(), interval_round_trips_seconds(), interval_to_duration(), MAX_POOL_CONNECTIONS, MIGRATIONS_TABLE, MIGRATOR

### Community 71 - "ModelManifestError"
Cohesion: 0.10
Nodes (20): RequirementError, ManifestHash, ManifestHashMismatch, ModelManifest, Parameters, ModelAssetRole, AudioVae, Diffusion (+12 more)

### Community 73 - "http.rs"
Cohesion: 0.47
Nodes (3): healthz(), readyz(), router()

### Community 74 - "gpq_native_generate"
Cohesion: 0.25
Nodes (12): backend_from_request(), CancelledError, clear_lora_warning_detail(), duplicate_string(), gpq_native_generate(), gpq_native_list_devices(), make_context_params(), required_string() (+4 more)

### Community 75 - "drain_worker_chunks"
Cohesion: 0.38
Nodes (9): DeliveryContext, discard_and_expire(), drain_worker_chunks(), fail_delivery(), mark_delivery_lost(), persist_delivery_offset(), request_worker_delivery(), resume_attempts_exhausted() (+1 more)

### Community 76 - "Active Runtime"
Cohesion: 0.50
Nodes (4): Active Runtime, Device Pool, Execution Slot, Worker configuration

### Community 77 - "gpq-domain"
Cohesion: 0.53
Nodes (6): gpq, gpq-domain, gpq-inference, gpq-proto, gpq-remote, gpq-worker

### Community 79 - "package.json"
Cohesion: 0.14
Nodes (13): devDependencies, @types/node, typescript, engines, node, name, private, scripts (+5 more)

### Community 86 - "main.rs"
Cohesion: 0.21
Nodes (8): Cli, Command, Infer, Remote, Worker, Forwarded, main(), role_argv()

### Community 88 - "duration"
Cohesion: 0.21
Nodes (6): manifest(), OUTPUT_ARTIFACT_TTL, TRANSFER_CHUNK_BYTES, verification_accepts_matching_bytes(), verification_rejects_short_transfer(), verification_rejects_wrong_digest()

### Community 89 - "DeliveryChunk"
Cohesion: 0.20
Nodes (7): DeliveryChunk, Complete, Data, Failed, Rejected, push_delivery_reports_no_subscriber(), service_without_object_store()

### Community 90 - "db.rs"
Cohesion: 0.41
Nodes (7): artifact_state(), attempt_rows(), AttemptRow, event_kinds(), generation_row(), GenerationRow, latest_generation_row()

### Community 91 - "Command"
Cohesion: 0.18
Nodes (12): Cli, Command, Diagnose, Enroll, Run, Service, ConfigArgs, ServiceCommand (+4 more)

### Community 92 - "connect_and_serve"
Cohesion: 0.43
Nodes (5): connect_and_serve(), run(), SessionError, Connection, ProtocolMismatch

### Community 93 - "AbortOnDrop"
Cohesion: 0.29
Nodes (3): AbortOnDrop, MaintenanceTask, run_maintenance_tick()

### Community 94 - "ArtifactState"
Cohesion: 0.12
Nodes (8): ArtifactState, terminal_attempt_states_are_final(), TransitionError, ArtifactStoreError, Conflict, NotFound, Sql, Transition

### Community 95 - "objectstore_support/mod.rs"
Cohesion: 0.10
Nodes (9): BUCKET, MINIO_IMAGE, MINIO_ROOT_PASSWORD, MINIO_ROOT_USER, MINIO_TAG, ObjectStoreFixture, PRESIGN_TTL_SECS, REGION (+1 more)

### Community 96 - "ProcessIdentity"
Cohesion: 0.24
Nodes (11): command_line_corroborates(), identity_file_round_trips(), missing_identity_file_reads_as_none(), mtime_unix(), ProcessIdentity, read_identity_file(), read_start_marker(), record_identity_file() (+3 more)

### Community 97 - "src/cli.rs"
Cohesion: 0.14
Nodes (16): BACKGROUND_TASK_SHUTDOWN_GRACE, run_cli(), run_migrate(), run_serve(), run_tenant(), run_tenant_key(), run_worker(), SHUTDOWN_DRAIN_TIMEOUT (+8 more)

### Community 98 - "SchedulerHandle"
Cohesion: 0.15
Nodes (8): connect_listener(), recv_or_pending(), run(), SchedulerHandle, spawn(), Wake, Tenant, Worker

### Community 99 - "gpq-remote/src/artifacts.rs"
Cohesion: 0.09
Nodes (14): as_u64(), BACKPRESSURE_RETRY_DELAY, DELIVERY_CHANNEL_CAPACITY, DELIVERY_CHUNK_TIMEOUT, download_maps_delivering_to_conflict(), download_maps_input_direction_to_not_found(), download_maps_object_store_available_to_redirect(), download_maps_offline_worker_local_to_worker_offline() (+6 more)

### Community 102 - "TenantCommand"
Cohesion: 0.18
Nodes (11): Cli, Command, Migrate, Serve, Tenant, Worker, TenantCommand, Create (+3 more)

### Community 103 - "accept_lease"
Cohesion: 0.22
Nodes (10): accept_lease(), classify_lease(), lease_seed_outside_native_range_is_rejected_before_pool_work(), lease_ttl_chrono(), lease_with_malformed_model_hash_is_rejected_before_pool_work(), LeaseRejection, Rejected, Unidentifiable (+2 more)

### Community 104 - "compilerOptions"
Cohesion: 0.18
Nodes (10): compilerOptions, erasableSyntaxOnly, module, noEmit, skipLibCheck, strict, target, types (+2 more)

### Community 105 - "ManifestMismatch"
Cohesion: 0.13
Nodes (14): ArtifactManifest, ManifestMismatch, Digest, Size, ArtifactChunkData, ArtifactReader, PublishError, Io (+6 more)

### Community 106 - "generation_row_to_proto"
Cohesion: 0.23
Nodes (4): discontinuity_event(), discontinuity_event_reports_the_given_reason(), generation_row_to_proto(), refreshed_snapshot_event()

### Community 107 - "LoraSet"
Cohesion: 0.22
Nodes (6): has_nonzero_lora(), LoraSet, values, SampleParamsOwner, sigmas, value

### Community 108 - "e2e_support/cli.rs"
Cohesion: 0.44
Nodes (6): migrate(), RotatedKey, run(), tenant_create(), tenant_key_rotate(), worker_revoke()

### Community 109 - "JobHandle"
Cohesion: 0.28
Nodes (4): command_line(), JobHandle, open_for_query(), read_command_line()

### Community 110 - "gpq-worker/src/session.rs"
Cohesion: 0.08
Nodes (7): backoff_base(), jittered_backoff(), jittered_backoff_stays_within_quarter_bounds_and_under_max(), MAINTENANCE_INTERVAL, MAX_RECONNECT_BACKOFF, pool_advertisement(), SHUTDOWN_DRAIN_TIMEOUT

### Community 111 - "handle_remote_message"
Cohesion: 0.25
Nodes (4): deliver_artifact(), deliver_artifact_message(), discard_output(), handle_remote_message()

### Community 112 - "ImageArray"
Cohesion: 0.25
Nodes (5): ImageArray, count, images, OwnedImage, view

### Community 113 - "ArtifactId"
Cohesion: 0.36
Nodes (6): deliver_request(), discard_output_request(), download_path(), download_url(), download_url_builds_an_absolute_url_from_the_public_base(), remote_local_upload_url()

### Community 114 - "Ownership"
Cohesion: 0.25
Nodes (8): decide(), kill_stale(), Ownership, Foreign, Gone, Owned, terminate_pid(), terminate_pid_tree()

### Community 115 - "Trust Remote and enforce database tenant isolation"
Cohesion: 0.29
Nodes (4): tenant trust boundaries, credential separation, row-level tenant isolation, migration and serving credentials

### Community 116 - "record_accepted_result"
Cohesion: 0.36
Nodes (4): domain_manifest_from_proto(), manifest_conversion_accepts_a_well_formed_manifest(), reject_oversized_outputs(), settle_attempt_failure()

### Community 117 - "ProgressRegistrationData"
Cohesion: 0.33
Nodes (5): progress_callback(), ProgressRegistration, ProgressRegistrationData, callback, user_data

### Community 118 - "wait_until"
Cohesion: 0.21
Nodes (10): wait_until(), harness_teardown_waits_for_active_uses(), heartbeat_renews_lease_past_the_ttl_without_expiry(), late_result_after_cancellation_is_rejected_and_discarded(), lease_and_start(), revoked_worker_credential_blocks_reconnection_and_scheduling(), session_handshake_rejects_incompatible_protocol_major(), stale_result_after_lease_expiry_is_rejected_and_discarded() (+2 more)

### Community 119 - "terminate_group"
Cohesion: 0.40
Nodes (4): create_and_assign(), group_alive(), signal_group(), terminate_group()

### Community 120 - "NativeStatus"
Cohesion: 0.33
Nodes (6): NativeStatus, NATIVE_STATUS_CANCELLED, NATIVE_STATUS_INVALID_REQUEST, NATIVE_STATUS_OK, NATIVE_STATUS_OUT_OF_MEMORY, NATIVE_STATUS_RUNTIME

### Community 121 - "gpq-domain/src/lib.rs"
Cohesion: 0.14
Nodes (7): MAX_IMAGE_REFERENCES, MAX_NATIVE_SEED, MAX_PROMPT_BYTES, SeedOutOfRange, validate_seed(), assert_serde_matches(), serde_names_match_stable_names()

### Community 122 - "classify_result_outcome"
Cohesion: 0.47
Nodes (6): accepted_outcome_carries_the_generation_id_and_is_not_discarded(), classify_result_outcome(), ResultReply, Accepted, AcceptedReplay, Discard

### Community 123 - "NativeEvent"
Cohesion: 0.33
Nodes (6): NativeEvent, Cancelled, Complete, Error, Output, Progress

### Community 126 - "PngFileSink"
Cohesion: 0.50
Nodes (3): PngFileSink, failed, file

### Community 127 - "HarnessEntry"
Cohesion: 0.21
Nodes (7): cleanup_harness_entry(), harness(), HarnessAccess, HarnessEntry, run_background(), schedule_retired_harness_cleanup(), teardown_harness()

### Community 129 - "ChunkDrainOutcome"
Cohesion: 0.67
Nodes (3): ChunkDrainOutcome, Done, ResumeDelivery

### Community 130 - "DeliveryRequestOutcome"
Cohesion: 0.67
Nodes (3): DeliveryRequestOutcome, Ready, Stop

### Community 135 - "native_exclusions.rs"
Cohesion: 0.56
Nodes (3): excluded_pools(), excluded_pools_for_generations(), record_preexecution_rejection()

### Community 136 - ".submit"
Cohesion: 0.25
Nodes (4): invalid(), lora_from_proto(), required_idempotency_key(), required_idempotency_key_trims_and_accepts_a_reasonable_key()

### Community 137 - "ContainerLease"
Cohesion: 0.33
Nodes (5): terminate_gracefully(), cleanup_requested_container(), ContainerLease, maintenance_url(), reap_shared_container()

### Community 138 - "DownloadOutcome"
Cohesion: 0.25
Nodes (8): classify_download(), DownloadOutcome, Conflict, Deliver, Gone, NotFound, Redirect, WorkerOffline

### Community 139 - ".ensure_runtime_and_acquire_slot"
Cohesion: 0.43
Nodes (3): internal_error(), PoolState, SlotLease

### Community 140 - "ArtifactDirection"
Cohesion: 0.29
Nodes (3): ArtifactDirection, Input, Output

### Community 141 - ".from_row"
Cohesion: 0.33
Nodes (3): decode(), decode_rejects_unknown_state_name(), decode_u64()

### Community 142 - "SendOutcome"
Cohesion: 0.40
Nodes (4): SendOutcome, Backpressured, Delivered, Offline

## Knowledge Gaps
- **238 isolated node(s):** `Size`, `Digest`, `ModelManifest`, `Parameters`, `ManifestHash` (+233 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 781 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **24 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `ContentHash` connect `db/catalog.rs` to `e2e.rs`, `workers.rs`, `generations.rs`, `.ensure_runtime_and_acquire_slot`, `gpq-inference/src/lib.rs`, `pool.rs`, `executor.rs`, `infer.rs`, `objectstore.rs`, `.register_workflow_version`, `transfer.rs`, `gpq-remote/src/session.rs`, `native_artifact_safety.rs`, `src/generation.rs`, `gpq-worker/src/artifacts.rs`, `gpq-worker/src/config.rs`, `IncapableReason`, `scheduler.rs`, `image_request`, `backend/mod.rs`, `native/generation.rs`, `src/models.rs`, `schedule.rs`, `super`, `db/artifacts.rs`, `plan_assignments`, `ModelManifest`, `capability.rs`, `lock_state`, `Hasher`, `ModelManifestError`, `drain_worker_chunks`, `duration`, `gpq-remote/src/artifacts.rs`, `ManifestMismatch`, `gpq-domain/src/lib.rs`?**
  _High betweenness centrality (0.136) - this node is a cross-community bridge._
- **What connects `Size`, `Digest`, `ModelManifest` to the rest of the system?**
  _238 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `e2e.rs` be split into smaller, more focused modules?**
  _Cohesion score 0.13513513513513514 - nodes in this community are weakly interconnected._
- **Why does `AppState` connect `AppState` to `ArtifactService`, `tenants.rs`, `GenerationEvent`, `.register_workflow_version`, `transfer.rs`, `gpq-remote/src/session.rs`, `native/mod.rs`, `WorkerRegistry`, `admission.rs`, `try_handle_attempt_failure`, `TenantId`, `scheduler.rs`, `Db`, `native/generation.rs`, `gpq-remote/src/config.rs`, `Response`, `plan_assignments`, `dispatch_worker_message`, `GenerationApi`, `http.rs`, `drain_worker_chunks`, `src/cli.rs`, `SchedulerHandle`, `gpq-remote/src/artifacts.rs`, `generation_row_to_proto`, `record_accepted_result`?**
  _High betweenness centrality (0.050) - this node is a cross-community bridge._
- **Should `workers.rs` be split into smaller, more focused modules?**
  _Cohesion score 0.14935988620199148 - nodes in this community are weakly interconnected._
- **Why does `ModelManifest` connect `ModelManifest` to `capability.rs`, `workers.rs`, `generations.rs`, `db/catalog.rs`, `ModelManifestError`, `gpq-worker/src/config.rs`, `scheduler.rs`, `gpq-inference/src/lib.rs`, `pool.rs`, `executor.rs`, `infer.rs`, `src/models.rs`, `schedule.rs`, `.register_workflow_version`, `gpq-remote/src/session.rs`, `gpq-domain/src/lib.rs`, `plan_assignments`, `native_artifact_safety.rs`?**
  _High betweenness centrality (0.034) - this node is a cross-community bridge._
- **Should `Harness` be split into smaller, more focused modules?**
  _Cohesion score 0.13043478260869565 - nodes in this community are weakly interconnected._