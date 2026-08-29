# Graph Report - gpq  (2026-08-30)

## Corpus Check
- 115 files · ~135,528 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 2852 nodes · 7278 edges · 86 communities (77 shown, 9 thin omitted)
- Extraction: 97% EXTRACTED · 3% INFERRED · 0% AMBIGUOUS · INFERRED: 187 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `0c48c743`
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
- CatalogApi
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
- ExecutionTarget
- service.rs
- fake_llama.rs
- gpq-worker/src/artifacts.rs
- record_accepted_result
- gpq-worker/src/main.rs
- ContentHash
- SlotCapability
- Modality
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
- auth.rs
- gpq-domain/src/lib.rs
- .new
- .enroll
- resolve_and_store_images
- Candidate
- sse.rs
- native/catalog.rs
- ApiError
- run_inbound_pump
- e2e_support/cli.rs
- Result
- fetch_http_image
- list_models
- db/mod.rs
- TelemetryGuard
- http.rs
- recv_or_pending
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
- modality.rs
- AGENTS.md

## God Nodes (most connected - your core abstractions)
1. `AppState` - 116 edges
2. `ContentHash` - 91 edges
3. `BackendError` - 46 edges
4. `Harness` - 42 edges
5. `ArtifactManifest` - 35 edges
6. `ArtifactRow` - 34 edges
7. `FailureKind` - 32 edges
8. `ApiError` - 29 edges
9. `PoolConfig` - 29 edges
10. `ExecutionContext` - 28 edges

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

## Communities (86 total, 9 thin omitted)

### Community 0 - "wait_until"
Cohesion: 0.05
Nodes (93): build_fixtures(), fake(), Fixtures, harness(), interrupt_cancels_running_image_generation(), object_store(), object_store_output_placement_lands_in_bucket_and_redirects(), register_workflow() (+85 more)

### Community 1 - "gpq-remote/src/artifacts.rs"
Cohesion: 0.06
Nodes (70): Bytes, ArtifactService, as_u64(), checked_grow(), ChunkDrainOutcome, classify_download(), deliver_request(), deliver_worker_local() (+62 more)

### Community 2 - "workers.rs"
Cohesion: 0.14
Nodes (44): build_slot_capability(), builds_slot_capability_with_incapable_versions_split_out(), claim_slot(), clear_session(), decode_backend_kind(), decode_hash(), enroll(), free_slots_clamps_negative_database_values_to_zero() (+36 more)

### Community 3 - "Harness"
Cohesion: 0.06
Nodes (55): CatalogServiceClient, ClientConfig, AdminCli, chat_parameters(), collect_sse_json(), database_url_for(), database_url_with_credentials(), enroll_worker() (+47 more)

### Community 4 - "PoolConfig"
Cohesion: 0.08
Nodes (52): check_device_overlap(), duration_secs(), ensure_state_dir(), parses_full_sample(), parses_mlx_dspark_pool(), PoolConfig, rejects_duplicate_pool_keys(), rejects_empty_pools() (+44 more)

### Community 5 - "generations.rs"
Cohesion: 0.07
Nodes (64): i16, Priority, PriorityOutOfRange, Default, Display, Error, Formatter, From (+56 more)

### Community 6 - "native/generation.rs"
Cohesion: 0.05
Nodes (64): ArtifactRef, CancelGenerationRequest, CancelGenerationResponse, admission_error(), artifact_row_to_proto(), discontinuity_event(), discontinuity_event_reports_the_given_reason(), domain_event_to_proto() (+56 more)

### Community 7 - "llama.rs"
Cohesion: 0.07
Nodes (62): ChatMessage, build_chat_request(), build_chat_request_forces_stream_and_passes_through_fields(), build_chat_request_injects_seed_overriding_existing(), build_chat_request_omits_seed_when_absent(), build_chat_request_rejects_non_object_parameters(), cancelled_error(), ChatChunk (+54 more)

### Community 8 - "gpq-worker/src/session.rs"
Cohesion: 0.06
Nodes (62): AbortOnDrop, accept_lease(), backoff_base(), cancel_all_attempts(), cancel_attempt(), cancel_expired_leases(), capability_report_message(), classify_lease() (+54 more)

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
Nodes (56): GenerationState, chat_completions(), chat_finish_reason(), ChatChoice, ChatCompletionChunk, ChatCompletionRequest, ChatCompletionResponse, ChatMessage (+48 more)

### Community 13 - "process.rs"
Cohesion: 0.08
Nodes (49): ChildStderr, ChildStdout, command_line(), command_line_corroborates(), command_line_corroboration_accepts_interpreter_launched_backends(), create_and_assign(), decide(), filetime_to_u64() (+41 more)

### Community 14 - "pool.rs"
Cohesion: 0.08
Nodes (37): acquire_fails_on_an_unready_pool(), acquire_never_exceeds_capacity(), exit_detection_marks_pool_unready_and_clears_capacity(), lock_state(), mark_unready_for_exit(), PoolAdvertisementData, PoolEntry, PoolState (+29 more)

### Community 15 - "executor.rs"
Cohesion: 0.05
Nodes (69): ArtifactManifest, manifest(), MediaKind, String, verification_accepts_matching_bytes(), verification_rejects_short_transfer(), verification_rejects_wrong_digest(), FailureKind (+61 more)

### Community 16 - "comfy_support/mod.rs"
Cohesion: 0.09
Nodes (47): base_graph(), checkpoint_name(), event(), FakeComfy, free(), handle_socket(), hang_graph(), has_node_class() (+39 more)

### Community 17 - "attempts.rs"
Cohesion: 0.12
Nodes (36): RetryDecision, AttemptState, acknowledge_cancel(), AttemptRow, create(), CreateAttemptError, expired_leases(), finish() (+28 more)

### Community 18 - "GenerationEvent"
Cohesion: 0.08
Nodes (48): append(), append_attempt_created(), EventKind, EventRow, latest(), load_since(), AttemptId, DateTime (+40 more)

### Community 19 - "backend/comfy.rs"
Cohesion: 0.06
Nodes (38): apply_override(), apply_parameters(), apply_parameters_overrides_addressed_node_input(), apply_parameters_places_seed_at_reserved_pointer(), apply_parameters_rejects_malformed_pointer(), apply_parameters_rejects_unknown_node_id(), apply_parameters_skips_seed_without_pointer(), checkpoint_name() (+30 more)

### Community 20 - "CatalogApi"
Cohesion: 0.11
Nodes (27): CatalogService, catalog_error(), CatalogApi, internal(), invalid(), ConnectError, Display, RequestContext (+19 more)

### Community 21 - "responses.rs"
Cohesion: 0.08
Nodes (49): usage_from_row(), UsageDto, build_response_object(), collect_input_artifacts(), completed_response_carries_output_text_and_usage(), CompletedEvent, create_response(), CreatedEvent (+41 more)

### Community 22 - "transfer.rs"
Cohesion: 0.07
Nodes (40): ArtifactChunk, a_buffer_exactly_a_multiple_of_the_chunk_size_ends_on_a_boundary(), a_single_chunk_fitting_exactly_is_marked_last(), build_chunks(), ChunkReceipt, chunks_cover_the_whole_buffer_from_zero(), DeliveryValidation, domain_manifest() (+32 more)

### Community 23 - "gpq-remote/src/session.rs"
Cohesion: 0.08
Nodes (26): accepted_outcome_carries_the_generation_id_and_is_not_discarded(), classify_result_outcome(), domain_backend_kind(), domain_failure_kind(), domain_manifest_from_proto(), domain_placement(), GenerationSnapshot, manifest_conversion_accepts_a_well_formed_manifest() (+18 more)

### Community 24 - "native/mod.rs"
Cohesion: 0.07
Nodes (42): artifact_placement_from_proto(), artifact_placement_round_trips_through_proto(), artifact_placement_to_proto(), artifact_state_to_proto(), authenticate(), backend_kind_maps_every_domain_variant(), backend_kind_to_proto(), duration_from_proto() (+34 more)

### Community 25 - "src/cli.rs"
Cohesion: 0.09
Nodes (37): Cli, Command, CancellationToken, Command, Option, Result, String, Uuid (+29 more)

### Community 26 - "mlx.rs"
Cohesion: 0.11
Nodes (22): BatchingMetrics, bounded_slots(), fake_mlx_server(), HealthResponse, MetricsResponse, MlxDsparkBackend, probes_and_executes_against_mlx_dspark_api(), resolve_target() (+14 more)

### Community 27 - "admission.rs"
Cohesion: 0.13
Nodes (36): a_model_and_a_workflow_alias_of_the_same_name_hash_differently(), AdmissionError, AdmissionRequest, admit(), AliasTarget, differing_parameters_hash_differently(), differing_seed_hashes_differently(), ensure_synchronous_capacity() (+28 more)

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

### Community 33 - "service.rs"
Cohesion: 0.18
Nodes (31): install(), install_linux(), install_macos(), install_windows(), launchd_plist_path(), launchd_plist_wires_program_arguments(), quote_systemd_word(), quote_windows_arg() (+23 more)

### Community 34 - "fake_llama.rs"
Cohesion: 0.13
Nodes (26): chat_completions(), FakeLlama, FakeMode, health(), lock(), props(), Arc, Duration (+18 more)

### Community 35 - "gpq-worker/src/artifacts.rs"
Cohesion: 0.11
Nodes (40): ManifestMismatch, Result, ArtifactChunkData, ArtifactHandle, ArtifactReader, delete_is_idempotent(), expire_removes_only_artifacts_past_ttl(), LocalArtifactStore (+32 more)

### Community 36 - "record_accepted_result"
Cohesion: 0.11
Nodes (41): AttemptFailure, AttemptProgress, AttemptResult, AttemptRunning, AttemptTokenDelta, CancelAcknowledged, CapabilityReport, generation_snapshot() (+33 more)

### Community 37 - "gpq-worker/src/main.rs"
Cohesion: 0.15
Nodes (28): Cli, combine_uninstall_results(), combine_uninstall_results_ok_when_both_succeed(), combine_uninstall_results_reports_both_failures_when_neither_step_succeeds(), combine_uninstall_results_surfaces_credential_failure_alone(), combine_uninstall_results_surfaces_service_failure_alone(), Command, ConfigArgs (+20 more)

### Community 38 - "ContentHash"
Cohesion: 0.13
Nodes (50): ContentHash, Display, CatalogError, content_hash_changes_with_graph(), content_hash_ignores_limits(), content_hash_is_stable_under_key_reordering(), decode_error(), decode_model_execution_limits() (+42 more)

### Community 39 - "SlotCapability"
Cohesion: 0.11
Nodes (32): BTreeSet, accelerator_memory_boundary_is_inclusive(), any_candidate_remains(), extra_models_and_custom_nodes_do_not_block_admission(), hash(), IncapableReason, missing_custom_node_reports_no_installed_version(), mlx_dspark_admits_llm_model_requirements() (+24 more)

### Community 40 - "Modality"
Cohesion: 0.13
Nodes (21): Modality, ExecutionLimits, ModelVersion, resolve_execution_timeout(), BTreeMap, Duration, MediaKind, Option (+13 more)

### Community 41 - "scheduler.rs"
Cohesion: 0.12
Nodes (22): build_lease_assignment(), duration_from_micros_round_trips(), duration_from_negative_micros_falls_back_to_zero(), lease_target_fields(), leased_output_key(), model_target_leaves_workflow_fields_unset(), proto_artifact_manifest(), proto_artifact_placement() (+14 more)

### Community 42 - "Db"
Cohesion: 0.13
Nodes (14): KeyedHasher, Vec, Db, DateTime, Option, PgPool, Postgres, Result (+6 more)

### Community 43 - "CredentialStore"
Cohesion: 0.20
Nodes (14): accepts_owner_only_file(), CredentialStore, current_uid(), load_unix_file_credential(), missing_file_is_none(), rejects_group_readable_file(), rejects_world_readable_file(), Option (+6 more)

### Community 44 - "openai/mod.rs"
Cohesion: 0.10
Nodes (13): ip_literal_host_public_address_is_accepted(), ipv6_mapped_private_is_rejected(), is_publicly_routable(), is_publicly_routable_v4(), is_publicly_routable_v6(), resolve_safe_addrs(), router(), Response (+5 more)

### Community 45 - "backend/mod.rs"
Cohesion: 0.11
Nodes (29): Backend, BackendCapabilities, build(), classify_transport_error(), client_error_status_normalizes_to_internal(), connect_failure_normalizes_to_backend_crashed(), ExecutionRequest, http_client() (+21 more)

### Community 46 - "BackendError"
Cohesion: 0.14
Nodes (23): ComfyBackend, custom_node_versions(), custom_node_versions_falls_back_to_unknown(), custom_node_versions_prefers_known_package_version(), internal_error(), ObjectInfoEntry, probe_ok(), resolve_checkpoint_path() (+15 more)

### Community 47 - "gpq-remote/src/config.rs"
Cohesion: 0.19
Nodes (15): credential_key_parses_valid_hex(), object_store_absent_is_none(), object_store_default_presign_ttl_is_fifteen_minutes(), object_store_full_configuration_resolves(), ObjectStoreConfig, parse_credential_key(), RemoteConfig, resolve_object_store() (+7 more)

### Community 48 - "plan_assignments"
Cohesion: 0.17
Nodes (20): assign(), eligible_pool_is_bounded_by_its_advertised_free_slots(), known_tenants(), plan_assignments(), pool_whose_worker_has_no_live_session_is_skipped(), pool_with_no_free_slots_is_skipped(), DateTime, DevicePoolId (+12 more)

### Community 49 - "src/models.rs"
Cohesion: 0.07
Nodes (47): CacheFile, digest_matches_known_vector(), Hasher, round_trips_through_hex(), D, Err, Error, Formatter (+39 more)

### Community 50 - "gpq-remote coordinator"
Cohesion: 0.13
Nodes (18): Accepted Result, Attempt, Generation, Tenant, Tenant Master Key, Worker, Worker Credential, ComfyUI (+10 more)

### Community 51 - "schedule.rs"
Cohesion: 0.24
Nodes (14): age_breaks_ties_within_equal_priority(), batches_share_the_pinned_version_and_respect_capacity(), empty_queue_yields_no_batch(), Fixture, hash(), incompatible_work_is_excluded(), no_free_slot_means_no_work(), overdue_selection_ignores_priority() (+6 more)

### Community 52 - "db.rs"
Cohesion: 0.34
Nodes (17): attempt_rows(), AttemptRow, db_now(), event_kinds(), generation_row(), generation_row_created_after(), GenerationRow, latest_generation_row() (+9 more)

### Community 53 - "auth.rs"
Cohesion: 0.18
Nodes (15): bearer_token(), bearer_token_is_none_without_a_header(), bearer_token_parses_case_insensitively(), bearer_token_parses_the_standard_scheme(), bearer_token_rejects_other_schemes(), different_secrets_hash_differently(), generate_secret(), generated_secrets_carry_their_prefix_and_are_unique() (+7 more)

### Community 54 - "gpq-domain/src/lib.rs"
Cohesion: 0.15
Nodes (8): lease_expires_forty_five_seconds_after_now(), lease_expiry_from(), DateTime, Utc, assert_serde_matches(), T, serde_names_match_stable_names(), Failure

### Community 55 - ".new"
Cohesion: 0.22
Nodes (9): internal(), not_renewed_attempts_each_get_a_cancel_request(), only_worker_local_placement_attributes_an_owning_worker(), ConnectError, Display, Self, SessionApi, unauthenticated() (+1 more)

### Community 56 - ".enroll"
Cohesion: 0.20
Nodes (12): EnrollmentApi, internal(), internal_anyhow(), ConnectError, EnrollResponse, Error, RequestContext, Self (+4 more)

### Community 57 - "resolve_and_store_images"
Cohesion: 0.21
Nodes (13): build_admission_request(), InputArtifactGuard, media_kind_from_mime(), resolve_and_store_images(), ArtifactId, Drop, IntoIterator, Item (+5 more)

### Community 58 - "Candidate"
Cohesion: 0.29
Nodes (10): Candidate, DateTime, Duration, GenerationId, Option, Utc, Vec, select_batch() (+2 more)

### Community 59 - "sse.rs"
Cohesion: 0.17
Nodes (24): advance(), BroadcastFrames, CancelOnDrop, data_event(), done_event(), finish_with(), named_event(), Drop (+16 more)

### Community 60 - "native/catalog.rs"
Cohesion: 0.21
Nodes (13): custom_node_without_exact_version_is_rejected(), missing_mime_type_is_rejected(), missing_output_node_is_rejected(), online_worker_count(), Result, String, valid_manifest(), validate_manifest() (+5 more)

### Community 61 - "ApiError"
Cohesion: 0.22
Nodes (12): ApiError, ErrorBody, ErrorDetail, failure_status_and_code(), Into, Option, Self, StatusCode (+4 more)

### Community 64 - "run_inbound_pump"
Cohesion: 0.16
Nodes (20): handle_heartbeat(), log_on_err(), not_renewed_cancel_messages(), E, Encodable, Future, InboundStream, Output (+12 more)

### Community 65 - "e2e_support/cli.rs"
Cohesion: 0.42
Nodes (11): migrate(), RotatedKey, Output, Path, Result, String, Uuid, run() (+3 more)

### Community 67 - "Result"
Cohesion: 0.23
Nodes (12): await_terminal_generation(), fetch_generation_row(), GenerationId, GenerationRow, Receiver, Result, TenantId, tenant_settings() (+4 more)

### Community 68 - "fetch_http_image"
Cohesion: 0.27
Nodes (9): data_url_decodes_base64(), data_url_decodes_percent_encoded_text(), data_url_enforces_size_cap(), fetch_http_image(), parse_data_url(), resolve_image_input(), ResolvedImage, Duration (+1 more)

### Community 69 - "list_models"
Cohesion: 0.29
Nodes (9): list_models(), ModelListResponse, ModelObject, Json, Result, State, String, Vec (+1 more)

### Community 70 - "db/mod.rs"
Cohesion: 0.39
Nodes (8): interval_days_and_microseconds_combine(), interval_days_are_folded_into_a_fixed_day(), interval_months_fold_into_thirty_day_periods(), interval_round_trips_seconds(), interval_to_duration(), Duration, Error, PgInterval

### Community 72 - "TelemetryGuard"
Cohesion: 0.38
Nodes (5): init(), Drop, Result, TelemetryGuard, SdkTracerProvider

### Community 73 - "http.rs"
Cohesion: 0.53
Nodes (5): healthz(), readyz(), router(), State, StatusCode

### Community 74 - "recv_or_pending"
Cohesion: 0.40
Nodes (6): connect_listener(), recv_or_pending(), Error, Option, PgListener, PgNotification

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
Cohesion: 0.22
Nodes (8): JoinHandle, Receiver, Self, Sender, run(), SchedulerHandle, spawn(), Wake

## Knowledge Gaps
- **59 isolated node(s):** `UsageJson`, `DeltaEvent`, `remote_session_start.sh script`, `graphify`, `tenant trust boundaries` (+54 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **9 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `AppState` connect `AppState` to `gpq-remote/src/artifacts.rs`, `native/generation.rs`, `tenants.rs`, `chat.rs`, `GenerationEvent`, `CatalogApi`, `responses.rs`, `transfer.rs`, `gpq-remote/src/session.rs`, `native/mod.rs`, `src/cli.rs`, `admission.rs`, `record_accepted_result`, `scheduler.rs`, `Db`, `openai/mod.rs`, `gpq-remote/src/config.rs`, `plan_assignments`, `.new`, `.enroll`, `resolve_and_store_images`, `sse.rs`, `native/catalog.rs`, `ApiError`, `run_inbound_pump`, `Result`, `list_models`, `http.rs`, `recv_or_pending`, `SchedulerHandle`?**
  _High betweenness centrality (0.292) - this node is a cross-community bridge._
- **Why does `ContentHash` connect `ContentHash` to `workers.rs`, `generations.rs`, `llama.rs`, `pool.rs`, `executor.rs`, `transfer.rs`, `mlx.rs`, `ExecutionTarget`, `gpq-worker/src/artifacts.rs`, `SlotCapability`, `Modality`, `backend/mod.rs`, `BackendError`, `plan_assignments`, `src/models.rs`, `schedule.rs`, `Candidate`, `native/catalog.rs`, `list_models`?**
  _High betweenness centrality (0.189) - this node is a cross-community bridge._
- **Why does `FailureKind` connect `executor.rs` to `record_accepted_result`, `generations.rs`, `Modality`, `gpq-worker/src/session.rs`, `backend/mod.rs`, `BackendError`, `attempts.rs`, `GenerationEvent`, `backend/comfy.rs`, `gpq-remote/src/session.rs`, `native/mod.rs`, `ApiError`?**
  _High betweenness centrality (0.093) - this node is a cross-community bridge._
- **What connects `UsageJson`, `DeltaEvent`, `remote_session_start.sh script` to the rest of the system?**
  _59 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `wait_until` be split into smaller, more focused modules?**
  _Cohesion score 0.0502970297029703 - nodes in this community are weakly interconnected._
- **Should `gpq-remote/src/artifacts.rs` be split into smaller, more focused modules?**
  _Cohesion score 0.06153846153846154 - nodes in this community are weakly interconnected._
- **Should `workers.rs` be split into smaller, more focused modules?**
  _Cohesion score 0.1374113475177305 - nodes in this community are weakly interconnected._