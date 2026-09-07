# Graph Report - gpq  (2026-09-07)

## Corpus Check
- 124 files · ~152,888 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 3115 nodes · 8188 edges · 104 communities (94 shown, 7 thin omitted)
- Extraction: 98% EXTRACTED · 2% INFERRED · 0% AMBIGUOUS · INFERRED: 196 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `9b046210`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- wait_until
- gpq-remote/src/artifacts.rs
- workers.rs
- Harness
- PoolConfig
- generations.rs
- native/generation.rs
- llama.rs
- gpq-worker/src/session.rs
- tenants.rs
- ArtifactRow
- postgres.rs
- chat.rs
- process.rs
- pool.rs
- executor.rs
- comfy_support/mod.rs
- attempts.rs
- GenerationEvent
- backend/comfy.rs
- .register_workflow_version
- responses.rs
- transfer.rs
- gpq-remote/src/session.rs
- native/mod.rs
- src/cli.rs
- mlx.rs
- admission.rs
- Offer compatible and native APIs
- AppState
- Test workflow
- Result
- db/comfy.rs
- service.rs
- fake_llama.rs
- gpq-worker/src/artifacts.rs
- record_accepted_result
- images.rs
- db/catalog.rs
- SlotCapability
- auth.rs
- scheduler.rs
- Db
- CredentialStore
- openai/mod.rs
- backend/mod.rs
- BackendError
- gpq-remote/src/config.rs
- plan_assignments
- src/models.rs
- gpq-remote coordinator
- schedule.rs
- db.rs
- expired_attempt_ids
- gpq-domain/src/lib.rs
- .new
- .enroll
- resolve_and_store_images
- Candidate
- sse.rs
- tenant_console.rs
- .new
- ContentHash
- Modality
- TenantId
- e2e_support/cli.rs
- tests/comfy.rs
- ws.rs
- PoolSupervisor
- comfy/mod.rs
- db/mod.rs
- package.json
- TelemetryGuard
- http.rs
- BackendKind
- main
- Active Runtime
- gpq-domain
- main
- Artifact
- Model
- Workflow
- Artifact download endpoint
- remote_session_start.sh
- SchedulerHandle
- objectstore.rs
- gpq-worker/src/main.rs
- compilerOptions
- ComfyBackend
- main.ts
- SessionError
- MaintenanceTask
- lease_expiry_from
- ObjectStoreFixture
- .invalid_request
- ApiError
- AGENTS.md
- Result
- lifecycle.rs
- parse_uuid_id
- settle_attempt_failure

## God Nodes (most connected - your core abstractions)
1. `AppState` - 141 edges
2. `ContentHash` - 97 edges
3. `BackendError` - 54 edges
4. `Harness` - 43 edges
5. `ArtifactRow` - 38 edges
6. `ArtifactManifest` - 35 edges
7. `ApiError` - 35 edges
8. `FailureKind` - 33 edges
9. `ComfyBackend` - 31 edges
10. `wait_until()` - 30 edges

## Surprising Connections (you probably didn't know these)
- `Buf` --references--> `Buf configuration`  [INFERRED]
  .github/workflows/test.yml → buf.yaml
- `Leased at-least-once execution` --conceptually_related_to--> `Attempt`  [INFERRED]
  README.md → CONTEXT.md
- `gpq-remote coordinator` --conceptually_related_to--> `Tenant`  [INFERRED]
  README.md → CONTEXT.md
- `Tenant administration` --references--> `Tenant Master Key`  [INFERRED]
  README.md → CONTEXT.md
- `gpq-worker service` --conceptually_related_to--> `Worker`  [INFERRED]
  README.md → CONTEXT.md

## Import Cycles
- 2-file cycle: `crates/gpq-remote/src/artifacts.rs -> crates/gpq-remote/src/state.rs -> crates/gpq-remote/src/artifacts.rs`
- 2-file cycle: `crates/gpq-remote/src/scheduler.rs -> crates/gpq-remote/src/state.rs -> crates/gpq-remote/src/scheduler.rs`
- 3-file cycle: `crates/gpq-remote/src/artifacts.rs -> crates/gpq-remote/src/http.rs -> crates/gpq-remote/src/state.rs -> crates/gpq-remote/src/artifacts.rs`

## Hyperedges (group relationships)
- **CI quality gate** — github_workflows_test_protobuf_lint, github_workflows_test_cargo_check, github_workflows_test_unit_tests, github_workflows_test_clippy [EXTRACTED 1.00]
- **Tenant generation worker execution flow** — context_tenant, context_generation, context_attempt, context_worker, context_accepted_result [EXTRACTED 1.00]
- **Generation execution and delivery** — docs_adr_0002_prioritize_gpu_utilization_prioritize_gpu_utilization, docs_adr_0003_use_leased_at_least_once_execution_use_leased_at_least_once_execution, docs_adr_0007_unify_generation_lifecycle_only_unify_the_generation_lifecycle_only, docs_adr_0008_keep_records_but_expire_artifacts_keep_records_but_expire_artifacts [INFERRED 0.85]
- **Tenant isolation and operations** — docs_adr_0001_tenant_dedicated_workers_keep_workers_tenant_dedicated, docs_adr_0009_separate_tenant_and_worker_credentials_separate_tenant_and_worker_credentials, docs_adr_0011_trust_remote_and_enforce_rls_trust_remote_and_enforce_database_tenant_isolation, docs_adr_0016_separate_migration_from_serving_separate_migration_from_serving, docs_adr_0018_do_not_sandbox_custom_nodes_do_not_sandbox_comfyui_custom_nodes [INFERRED 0.85]

## Communities (104 total, 7 thin omitted)

### Community 0 - "wait_until"
Cohesion: 0.14
Nodes (30): backend_crash_recovery_restores_pool_readiness(), backend_failure_exhausts_retries_and_fails_generation(), chat_completion_succeeds_with_one_attempt_and_persists_events(), client_disconnect_cancels_generation_and_attempt(), count_nonterminal(), cross_tenant_isolation_over_the_api(), delete_model_alias_removes_listing_without_mutating_pinned_generations(), get_tenant_settings_returns_migration_defaults() (+22 more)

### Community 1 - "gpq-remote/src/artifacts.rs"
Cohesion: 0.06
Nodes (73): Bytes, ArtifactService, as_u64(), checked_grow(), ChunkDrainOutcome, classify_download(), consume_worker_local_output(), deliver_request() (+65 more)

### Community 2 - "workers.rs"
Cohesion: 0.14
Nodes (44): build_slot_capability(), builds_slot_capability_with_incapable_versions_split_out(), claim_slot(), clear_session(), decode_backend_kind(), decode_hash(), enroll(), free_slots_clamps_negative_database_values_to_zero() (+36 more)

### Community 3 - "Harness"
Cohesion: 0.06
Nodes (55): CatalogServiceClient, ClientConfig, AdminCli, chat_parameters(), collect_sse_json(), database_url_for(), database_url_with_credentials(), enroll_worker() (+47 more)

### Community 4 - "PoolConfig"
Cohesion: 0.08
Nodes (55): check_device_overlap(), duration_secs(), ensure_state_dir(), parses_custom_node_versions(), parses_full_sample(), parses_mlx_dspark_pool(), PoolConfig, rejects_backend_url_query_and_fragment() (+47 more)

### Community 5 - "generations.rs"
Cohesion: 0.08
Nodes (64): i16, Priority, PriorityOutOfRange, Default, Display, Error, Formatter, From (+56 more)

### Community 6 - "native/generation.rs"
Cohesion: 0.05
Nodes (64): ArtifactRef, CancelGenerationRequest, CancelGenerationResponse, admission_error(), artifact_row_to_proto(), discontinuity_event(), discontinuity_event_reports_the_given_reason(), domain_event_to_proto() (+56 more)

### Community 7 - "llama.rs"
Cohesion: 0.07
Nodes (61): ChatMessage, build_chat_request(), build_chat_request_forces_stream_and_passes_through_fields(), build_chat_request_injects_seed_overriding_existing(), build_chat_request_omits_seed_when_absent(), build_chat_request_rejects_non_object_parameters(), ChatChunk, ChatChunkChoice (+53 more)

### Community 8 - "gpq-worker/src/session.rs"
Cohesion: 0.08
Nodes (40): accept_lease(), backoff_base(), cancel_attempt(), capability_report_message(), classify_lease(), connect_and_serve(), deliver_artifact(), deliver_artifact_message() (+32 more)

### Community 9 - "tenants.rs"
Cohesion: 0.07
Nodes (61): overdue_is_inclusive_of_the_boundary(), Default, Duration, Self, TenantSettings, create_tenant(), decode_error(), delete_tenant() (+53 more)

### Community 10 - "ArtifactRow"
Cohesion: 0.07
Nodes (46): ArtifactPlacement, ArtifactState, Result, Self, terminal_attempt_states_are_final(), TransitionError, ArtifactDirection, ArtifactRow (+38 more)

### Community 11 - "postgres.rs"
Cohesion: 0.11
Nodes (61): accept_result_rejects_a_result_for_a_terminal_generation(), accept_result_rejects_a_result_under_an_expired_lease(), accept_result_rejects_a_second_result_once_accepted(), accept_result_settles_the_first_result_and_releases_the_slot(), active_tenants_excludes_soft_deleted_tenants(), artifact_lifecycle_available_to_delivering_to_consumed(), artifact_output_expires_one_hour_after_completion(), artifact_second_delivery_attempt_loses_the_conflict() (+53 more)

### Community 12 - "chat.rs"
Cohesion: 0.07
Nodes (58): GenerationState, chat_completions(), chat_finish_reason(), ChatChoice, ChatCompletionChunk, ChatCompletionRequest, ChatCompletionResponse, ChatMessage (+50 more)

### Community 13 - "process.rs"
Cohesion: 0.08
Nodes (49): ChildStderr, ChildStdout, command_line(), command_line_corroborates(), command_line_corroboration_accepts_interpreter_launched_backends(), create_and_assign(), decide(), filetime_to_u64() (+41 more)

### Community 14 - "pool.rs"
Cohesion: 0.10
Nodes (14): Backend, Send, Sync, acquire_fails_on_an_unready_pool(), acquire_never_exceeds_capacity(), exit_detection_marks_pool_unready_and_clears_capacity(), release_restores_exactly_one_slot_and_tracks_busy_attempts(), releasing_an_attempt_twice_never_grows_free_slots_past_total() (+6 more)

### Community 15 - "executor.rs"
Cohesion: 0.05
Nodes (73): ArtifactManifest, manifest(), MediaKind, Self, String, verification_accepts_matching_bytes(), verification_rejects_short_transfer(), verification_rejects_wrong_digest() (+65 more)

### Community 16 - "comfy_support/mod.rs"
Cohesion: 0.09
Nodes (47): base_graph(), checkpoint_name(), event(), FakeComfy, free(), handle_socket(), hang_graph(), has_node_class() (+39 more)

### Community 17 - "attempts.rs"
Cohesion: 0.12
Nodes (36): RetryDecision, AttemptState, acknowledge_cancel(), AttemptRow, create(), CreateAttemptError, expired_leases(), finish() (+28 more)

### Community 18 - "GenerationEvent"
Cohesion: 0.07
Nodes (49): append(), append_attempt_created(), EventKind, EventRow, latest(), load_since(), AttemptId, DateTime (+41 more)

### Community 19 - "backend/comfy.rs"
Cohesion: 0.06
Nodes (30): apply_override(), apply_parameters(), apply_parameters_overrides_addressed_node_input(), apply_parameters_places_seed_at_reserved_pointer(), apply_parameters_rejects_malformed_pointer(), apply_parameters_rejects_missing_portable_placeholder(), apply_parameters_rejects_unknown_node_id(), apply_parameters_replaces_portable_placeholders() (+22 more)

### Community 20 - ".register_workflow_version"
Cohesion: 0.11
Nodes (27): CatalogService, catalog_error(), CatalogApi, internal(), invalid(), ConnectError, Display, RequestContext (+19 more)

### Community 21 - "responses.rs"
Cohesion: 0.08
Nodes (47): build_response_object(), collect_input_artifacts(), completed_response_carries_output_text_and_usage(), CompletedEvent, create_response(), CreatedEvent, CreateResponseRequest, DeltaEvent (+39 more)

### Community 22 - "transfer.rs"
Cohesion: 0.07
Nodes (40): ArtifactChunk, a_buffer_exactly_a_multiple_of_the_chunk_size_ends_on_a_boundary(), a_single_chunk_fitting_exactly_is_marked_last(), build_chunks(), ChunkReceipt, chunks_cover_the_whole_buffer_from_zero(), DeliveryValidation, domain_manifest() (+32 more)

### Community 23 - "gpq-remote/src/session.rs"
Cohesion: 0.09
Nodes (17): accepted_outcome_carries_the_generation_id_and_is_not_discarded(), classify_result_outcome(), domain_backend_kind(), domain_manifest_from_proto(), manifest_conversion_accepts_a_well_formed_manifest(), pool_upsert_from_proto(), pool_upsert_takes_total_slots_and_ignores_worker_busy_flags(), pool_upsert_treats_zero_accelerator_memory_as_unknown() (+9 more)

### Community 24 - "native/mod.rs"
Cohesion: 0.07
Nodes (41): artifact_placement_from_proto(), artifact_placement_round_trips_through_proto(), artifact_placement_to_proto(), artifact_state_to_proto(), authenticate(), backend_kind_maps_every_domain_variant(), backend_kind_to_proto(), duration_from_proto() (+33 more)

### Community 25 - "src/cli.rs"
Cohesion: 0.09
Nodes (37): Cli, Command, CancellationToken, Command, Option, Result, String, Uuid (+29 more)

### Community 26 - "mlx.rs"
Cohesion: 0.10
Nodes (28): cancelled_error(), BatchingMetrics, bound_model_hash_is_reused_until_the_directory_changes(), bounded_slots(), CancelHashOnDrop, fake_mlx_server(), HealthResponse, MetricsResponse (+20 more)

### Community 27 - "admission.rs"
Cohesion: 0.13
Nodes (37): a_model_and_a_workflow_alias_of_the_same_name_hash_differently(), AdmissionError, AdmissionRequest, AdmissionTarget, admit(), differing_parameters_hash_differently(), differing_seed_hashes_differently(), ensure_synchronous_capacity() (+29 more)

### Community 28 - "Offer compatible and native APIs"
Cohesion: 0.06
Nodes (40): Keep workers tenant-dedicated, tenant trust boundaries, cache-aware GPU scheduling, Prioritize GPU utilization, leased at-least-once delivery, Use leased at-least-once execution, public and worker transports, Separate public and Worker transports (+32 more)

### Community 29 - "AppState"
Cohesion: 0.13
Nodes (34): cancel_request_carries_the_attempt_id_and_reason(), cancel_request_message(), cancel_synchronous_for_tenant(), cancel_synchronous_on_startup(), discard_input_artifacts(), enforce_execution_deadlines(), expire_artifacts(), expire_leases() (+26 more)

### Community 30 - "Test workflow"
Cohesion: 0.07
Nodes (36): Buf breaking check, Buf configuration, Buf lint, proto module, STANDARD lint rules, Build and release job, Docker Buildx, Docker image build and push (+28 more)

### Community 31 - "Result"
Cohesion: 0.12
Nodes (25): enroll(), open_session(), random_content_sha256(), CancelRequest, ConnectError, DiscardOutput, Duration, Harness (+17 more)

### Community 32 - "db/comfy.rs"
Cohesion: 0.26
Nodes (25): artifact_view_matches(), ComfyHistoryRow, ComfyPromptRow, count_nonterminal(), count_terminal(), get_by_generation(), get_by_prompt_id(), history_by_prompt_id() (+17 more)

### Community 33 - "service.rs"
Cohesion: 0.18
Nodes (31): install(), install_linux(), install_macos(), install_windows(), launchd_plist_path(), launchd_plist_wires_program_arguments(), quote_systemd_word(), quote_windows_arg() (+23 more)

### Community 34 - "fake_llama.rs"
Cohesion: 0.13
Nodes (26): chat_completions(), FakeLlama, FakeMode, health(), lock(), props(), Arc, Duration (+18 more)

### Community 35 - "gpq-worker/src/artifacts.rs"
Cohesion: 0.11
Nodes (39): ManifestMismatch, Result, ArtifactChunkData, ArtifactHandle, ArtifactReader, delete_is_idempotent(), expire_removes_only_artifacts_past_ttl(), LocalArtifactStore (+31 more)

### Community 36 - "record_accepted_result"
Cohesion: 0.14
Nodes (31): AttemptResult, AttemptRunning, CancelAcknowledged, collect_comfy_view_refs(), ComfyViewRef, domain_placement(), generation_snapshot(), GenerationSnapshot (+23 more)

### Community 37 - "images.rs"
Cohesion: 0.19
Nodes (22): create_image(), CreateImageRequest, image_count(), image_size(), ImageData, ImagesResponse, openai_fields_become_workflow_placeholders(), output_artifacts() (+14 more)

### Community 38 - "db/catalog.rs"
Cohesion: 0.14
Nodes (48): CatalogError, content_hash_changes_with_graph(), content_hash_ignores_limits(), content_hash_is_stable_under_key_reordering(), decode_error(), decode_model_execution_limits(), decode_workflow_manifest(), delete_model_alias() (+40 more)

### Community 39 - "SlotCapability"
Cohesion: 0.14
Nodes (29): BTreeSet, accelerator_memory_boundary_is_inclusive(), any_candidate_remains(), extra_models_and_custom_nodes_do_not_block_admission(), IncapableReason, missing_custom_node_reports_no_installed_version(), mlx_dspark_admits_llm_model_requirements(), oom_marking_removes_the_slot_from_candidates() (+21 more)

### Community 40 - "auth.rs"
Cohesion: 0.18
Nodes (15): bearer_token(), bearer_token_is_none_without_a_header(), bearer_token_parses_case_insensitively(), bearer_token_parses_the_standard_scheme(), bearer_token_rejects_other_schemes(), different_secrets_hash_differently(), generate_secret(), generated_secrets_carry_their_prefix_and_are_unique() (+7 more)

### Community 41 - "scheduler.rs"
Cohesion: 0.13
Nodes (21): build_lease_assignment(), duration_from_micros_round_trips(), duration_from_negative_micros_falls_back_to_zero(), lease_target_fields(), leased_output_key(), model_target_leaves_workflow_fields_unset(), proto_artifact_manifest(), proto_artifact_placement() (+13 more)

### Community 42 - "Db"
Cohesion: 0.13
Nodes (14): KeyedHasher, Vec, Db, DateTime, Option, PgPool, Postgres, Result (+6 more)

### Community 43 - "CredentialStore"
Cohesion: 0.20
Nodes (14): accepts_owner_only_file(), CredentialStore, current_uid(), load_unix_file_credential(), missing_file_is_none(), rejects_group_readable_file(), rejects_world_readable_file(), Option (+6 more)

### Community 44 - "openai/mod.rs"
Cohesion: 0.11
Nodes (14): data_url_decodes_base64(), data_url_decodes_percent_encoded_text(), failure_status_and_code(), ip_literal_host_public_address_is_accepted(), is_publicly_routable(), is_publicly_routable_v4(), is_publicly_routable_v6(), resolve_safe_addrs() (+6 more)

### Community 45 - "backend/mod.rs"
Cohesion: 0.13
Nodes (27): BackendCapabilities, build(), classify_transport_error(), client_error_status_normalizes_to_internal(), connect_failure_normalizes_to_backend_crashed(), ExecutionEvent, ExecutionRequest, http_client() (+19 more)

### Community 46 - "BackendError"
Cohesion: 0.17
Nodes (31): classify_prompt_validation_error(), collect_raw_output_refs(), ComfyPackageVersion, default_view_kind(), extract_output_entries(), history_output(), internal_error(), parse_raw_prompt() (+23 more)

### Community 47 - "gpq-remote/src/config.rs"
Cohesion: 0.19
Nodes (15): credential_key_parses_valid_hex(), object_store_absent_is_none(), object_store_default_presign_ttl_is_fifteen_minutes(), object_store_full_configuration_resolves(), ObjectStoreConfig, parse_credential_key(), RemoteConfig, resolve_object_store() (+7 more)

### Community 48 - "plan_assignments"
Cohesion: 0.17
Nodes (20): assign(), eligible_pool_is_bounded_by_its_advertised_free_slots(), known_tenants(), plan_assignments(), pool_whose_worker_has_no_live_session_is_skipped(), pool_with_no_free_slots_is_skipped(), DateTime, DevicePoolId (+12 more)

### Community 49 - "src/models.rs"
Cohesion: 0.11
Nodes (48): CacheFile, digest_matches_known_vector(), Hasher, round_trips_through_hex(), FromStr, streaming_matches_one_shot(), cache_hits_for_an_unchanged_file(), cache_key() (+40 more)

### Community 50 - "gpq-remote coordinator"
Cohesion: 0.13
Nodes (18): Accepted Result, Attempt, Generation, Tenant, Tenant Master Key, Worker, Worker Credential, ComfyUI (+10 more)

### Community 51 - "schedule.rs"
Cohesion: 0.21
Nodes (16): age_breaks_ties_within_equal_priority(), batches_share_the_pinned_version_and_respect_capacity(), empty_queue_yields_no_batch(), Fixture, hash(), incompatible_work_is_excluded(), no_free_slot_means_no_work(), overdue_selection_ignores_priority() (+8 more)

### Community 52 - "db.rs"
Cohesion: 0.34
Nodes (17): attempt_rows(), AttemptRow, db_now(), event_kinds(), generation_row(), generation_row_created_after(), GenerationRow, latest_generation_row() (+9 more)

### Community 53 - "expired_attempt_ids"
Cohesion: 0.26
Nodes (12): cancel_all_attempts(), cancel_expired_leases(), expired_attempt_ids(), live_attempt_ids(), LiveAttempt, renew_leases(), AttemptId, DateTime (+4 more)

### Community 54 - "gpq-domain/src/lib.rs"
Cohesion: 0.12
Nodes (5): assert_serde_matches(), T, serde_names_match_stable_names(), Duration, Failure

### Community 55 - ".new"
Cohesion: 0.10
Nodes (20): internal(), log_on_err(), not_renewed_attempts_each_get_a_cancel_request(), only_worker_local_placement_attributes_an_owning_worker(), raw_generation_row(), ConnectError, Display, E (+12 more)

### Community 56 - ".enroll"
Cohesion: 0.20
Nodes (12): EnrollmentApi, internal(), internal_anyhow(), ConnectError, EnrollResponse, Error, RequestContext, Self (+4 more)

### Community 57 - "resolve_and_store_images"
Cohesion: 0.27
Nodes (10): build_admission_request(), InputArtifactGuard, resolve_and_store_images(), ArtifactId, Drop, IntoIterator, Item, Value (+2 more)

### Community 58 - "Candidate"
Cohesion: 0.36
Nodes (8): Candidate, DateTime, Option, Utc, Vec, select_batch(), select_next(), SlotContext

### Community 59 - "sse.rs"
Cohesion: 0.17
Nodes (24): advance(), BroadcastFrames, CancelOnDrop, data_event(), done_event(), finish_with(), named_event(), Drop (+16 more)

### Community 60 - "tenant_console.rs"
Cohesion: 0.32
Nodes (7): console_route_serves_the_browser_client_with_security_headers(), index(), router(), HeaderValue, Result, HeaderName, Html

### Community 61 - ".new"
Cohesion: 0.26
Nodes (6): ipv6_mapped_private_is_rejected(), router(), Into, Self, Parts, Rejection

### Community 62 - "ContentHash"
Cohesion: 0.09
Nodes (14): hash(), ExecutionTarget, ContentHash, D, Display, Err, Error, Formatter (+6 more)

### Community 63 - "Modality"
Cohesion: 0.09
Nodes (32): Modality, ExecutionLimits, ModelVersion, resolve_execution_timeout(), BTreeMap, Duration, Option, String (+24 more)

### Community 64 - "TenantId"
Cohesion: 0.22
Nodes (22): AttemptProgress, AttemptTokenDelta, CapabilityReport, discard_worker_outputs(), handle_capability_report(), handle_heartbeat(), load_tenant_settings(), not_renewed_cancel_messages() (+14 more)

### Community 65 - "e2e_support/cli.rs"
Cohesion: 0.42
Nodes (11): migrate(), RotatedKey, Output, Path, Result, String, Uuid, run() (+3 more)

### Community 66 - "tests/comfy.rs"
Cohesion: 0.24
Nodes (30): comfy_api_auth_and_client_isolation(), comfy_api_durable_admission_and_async_failure(), comfy_api_round_trip_preserves_mixed_outputs(), fake(), harness(), interrupt_cancels_running_image_generation(), object_store(), object_store_output_placement_lands_in_bucket_and_redirects() (+22 more)

### Community 67 - "ws.rs"
Cohesion: 0.22
Nodes (30): emit_event(), emit_success(), emit_terminal_from_row(), fetch_history_row(), maybe_send_status(), process_notification(), PromptTrack, remove_scanned_terminal_tracks() (+22 more)

### Community 68 - "PoolSupervisor"
Cohesion: 0.14
Nodes (23): lock_state(), mark_unready_for_exit(), PoolAdvertisementData, PoolEntry, PoolState, PoolSupervisor, Arc, AttemptId (+15 more)

### Community 69 - "comfy/mod.rs"
Cohesion: 0.09
Nodes (57): canonical_uuid(), ComfyError, error_response(), ErrorBody, ErrorDetail, get_history(), get_history_prompt(), get_prompt() (+49 more)

### Community 70 - "db/mod.rs"
Cohesion: 0.39
Nodes (8): interval_days_and_microseconds_combine(), interval_days_are_folded_into_a_fixed_day(), interval_months_fold_into_thirty_day_periods(), interval_round_trips_seconds(), interval_to_duration(), Duration, Error, PgInterval

### Community 71 - "package.json"
Cohesion: 0.11
Nodes (18): ai, @ai-sdk/openai-compatible, dependencies, ai, @ai-sdk/openai-compatible, devDependencies, @types/node, typescript (+10 more)

### Community 72 - "TelemetryGuard"
Cohesion: 0.38
Nodes (5): init(), Drop, Result, TelemetryGuard, SdkTracerProvider

### Community 73 - "http.rs"
Cohesion: 0.53
Nodes (5): healthz(), readyz(), router(), State, StatusCode

### Community 74 - "BackendKind"
Cohesion: 0.24
Nodes (8): BackendKind, Self, backend_kind_to_proto(), ensure_registered_resident_model(), resident_model_matches_registered(), Option, PathBuf, runtime_satisfies()

### Community 75 - "main"
Cohesion: 0.40
Nodes (4): main(), Box, Error, Result

### Community 76 - "Active Runtime"
Cohesion: 0.50
Nodes (4): Active Runtime, Device Pool, Execution Slot, Worker configuration

### Community 77 - "gpq-domain"
Cohesion: 0.67
Nodes (4): gpq-domain, gpq-proto, gpq-remote, gpq-worker

### Community 86 - "SchedulerHandle"
Cohesion: 0.14
Nodes (14): connect_listener(), recv_or_pending(), Error, JoinHandle, Option, Receiver, Self, Sender (+6 more)

### Community 87 - "objectstore.rs"
Cohesion: 0.28
Nodes (20): block_on(), Output, runtime(), chat_parameters(), create_and_upload_input(), create_input_artifact_presigns_upload_and_is_downloadable(), create_input_artifact_rejects_oversized_manifest(), expiry_sweep_transitions_and_deletes_past_due_object_store_artifact() (+12 more)

### Community 88 - "gpq-worker/src/main.rs"
Cohesion: 0.15
Nodes (28): Cli, combine_uninstall_results(), combine_uninstall_results_ok_when_both_succeed(), combine_uninstall_results_reports_both_failures_when_neither_step_succeeds(), combine_uninstall_results_surfaces_credential_failure_alone(), combine_uninstall_results_surfaces_service_failure_alone(), Command, ConfigArgs (+20 more)

### Community 89 - "compilerOptions"
Cohesion: 0.15
Nodes (12): compilerOptions, erasableSyntaxOnly, module, noEmit, skipLibCheck, strict, target, types (+4 more)

### Community 90 - "ComfyBackend"
Cohesion: 0.14
Nodes (17): ComfyBackend, custom_node_versions(), custom_node_versions_falls_back_to_unknown(), custom_node_versions_prefer_operator_configuration(), custom_node_versions_prefers_known_package_version(), ObjectInfoEntry, probe_ok(), revalidate_uses_workflow_manifest_model_pins() (+9 more)

### Community 91 - "main.ts"
Cohesion: 0.33
Nodes (4): gpq, prompt, size, { values, positionals }

### Community 92 - "SessionError"
Cohesion: 0.29
Nodes (7): expired_attempt_ids_is_empty_when_every_lease_is_live(), expired_attempt_ids_treats_exact_expiry_as_expired(), ConnectError, Error, From, Self, SessionError

### Community 93 - "MaintenanceTask"
Cohesion: 0.29
Nodes (6): AbortOnDrop, MaintenanceTask, Drop, JoinHandle, Option, run_maintenance_tick()

### Community 94 - "lease_expiry_from"
Cohesion: 0.50
Nodes (4): lease_expires_forty_five_seconds_after_now(), lease_expiry_from(), DateTime, Utc

### Community 95 - "ObjectStoreFixture"
Cohesion: 0.14
Nodes (14): build_fixtures(), Fixtures, Harness, try_build_fixtures(), ObjectStoreFixture, Client, ContainerAsync, Option (+6 more)

### Community 96 - ".invalid_request"
Cohesion: 0.33
Nodes (8): data_url_enforces_size_cap(), fetch_http_image(), parse_data_url(), resolve_image_input(), ResolvedImage, Duration, String, Url

### Community 99 - "ApiError"
Cohesion: 0.25
Nodes (7): ApiError, ErrorBody, ErrorDetail, Option, Response, validate_n(), IntoResponse

### Community 102 - "Result"
Cohesion: 0.39
Nodes (9): await_terminal_generation(), fetch_generation_row(), GenerationId, GenerationRow, Receiver, Result, TenantId, store_inline_input_artifact() (+1 more)

### Community 103 - "lifecycle.rs"
Cohesion: 0.39
Nodes (8): heartbeat_renews_lease_past_the_ttl_without_expiry(), late_result_after_cancellation_is_rejected_and_discarded(), Result, session_handshake_rejects_incompatible_protocol_major(), stale_result_after_lease_expiry_is_rejected_and_discarded(), worker_loss_lease_expiry_and_retry_succeeds_on_restart(), zy_revoked_worker_credential_blocks_reconnection_and_scheduling(), zzz_teardown_harness()

### Community 110 - "parse_uuid_id"
Cohesion: 0.20
Nodes (11): AttemptFailure, domain_failure_kind(), parse_uuid_id(), Error, FnOnce, T, Uuid, try_handle_attempt_failure() (+3 more)

### Community 116 - "settle_attempt_failure"
Cohesion: 0.43
Nodes (7): raw_attempt_output(), reject_oversized_outputs(), AttemptId, AttemptOutput, DateTime, Utc, settle_attempt_failure()

## Knowledge Gaps
- **82 isolated node(s):** `UsageJson`, `DeltaEvent`, `{ values, positionals }`, `size`, `gpq` (+77 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 679 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **7 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `AppState` connect `AppState` to `gpq-remote/src/artifacts.rs`, `native/generation.rs`, `tenants.rs`, `chat.rs`, `GenerationEvent`, `.register_workflow_version`, `responses.rs`, `transfer.rs`, `gpq-remote/src/session.rs`, `native/mod.rs`, `src/cli.rs`, `admission.rs`, `record_accepted_result`, `images.rs`, `scheduler.rs`, `Db`, `openai/mod.rs`, `gpq-remote/src/config.rs`, `plan_assignments`, `.new`, `.enroll`, `resolve_and_store_images`, `sse.rs`, `.new`, `Modality`, `TenantId`, `ws.rs`, `comfy/mod.rs`, `http.rs`, `SchedulerHandle`, `Result`, `parse_uuid_id`, `settle_attempt_failure`?**
  _High betweenness centrality (0.280) - this node is a cross-community bridge._
- **Why does `ContentHash` connect `ContentHash` to `workers.rs`, `generations.rs`, `llama.rs`, `pool.rs`, `executor.rs`, `transfer.rs`, `mlx.rs`, `gpq-worker/src/artifacts.rs`, `db/catalog.rs`, `SlotCapability`, `backend/mod.rs`, `BackendError`, `plan_assignments`, `src/models.rs`, `schedule.rs`, `Candidate`, `Modality`, `PoolSupervisor`, `comfy/mod.rs`, `BackendKind`?**
  _High betweenness centrality (0.218) - this node is a cross-community bridge._
- **Why does `ArtifactManifest` connect `executor.rs` to `gpq-remote/src/artifacts.rs`, `gpq-worker/src/artifacts.rs`, `scheduler.rs`, `ArtifactRow`, `backend/mod.rs`, `transfer.rs`, `gpq-remote/src/session.rs`, `objectstore.rs`, `ContentHash`?**
  _High betweenness centrality (0.101) - this node is a cross-community bridge._
- **What connects `UsageJson`, `DeltaEvent`, `{ values, positionals }` to the rest of the system?**
  _82 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `wait_until` be split into smaller, more focused modules?**
  _Cohesion score 0.13548387096774195 - nodes in this community are weakly interconnected._
- **Should `gpq-remote/src/artifacts.rs` be split into smaller, more focused modules?**
  _Cohesion score 0.060398078242964996 - nodes in this community are weakly interconnected._
- **Should `workers.rs` be split into smaller, more focused modules?**
  _Cohesion score 0.1374113475177305 - nodes in this community are weakly interconnected._