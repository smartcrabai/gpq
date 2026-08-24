# Graph Report - gpq  (2026-08-24)

## Corpus Check
- 116 files · ~132,258 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 2791 nodes · 7095 edges · 86 communities (79 shown, 7 thin omitted)
- Extraction: 98% EXTRACTED · 2% INFERRED · 0% AMBIGUOUS · INFERRED: 156 edges (avg confidence: 0.85)
- Token cost: 136,448 input · 11,328 output

## Community Hubs (Navigation)
- E2E ComfyUI Tests
- Artifact Service
- Capability Admission Tests
- Remote E2E Support
- Worker Configuration
- Priority Value Object
- Native Generation API
- Llama Backend API
- Worker Session
- Tenant Administration
- Domain State Machines
- PostgreSQL Integration Tests
- OpenAI Chat API
- Process Management
- Worker Pool Scheduling
- Execution Lifecycle
- Fake ComfyUI Backend
- Retry and Lease Logic
- Event Persistence
- ComfyUI Backend
- Catalog Service
- OpenAI Responses API
- Artifact Transfer
- Worker Session Protocol
- Native Protobuf Conversion
- Remote CLI
- ComfyUI Execution
- Generation Admission
- Architecture Decisions
- Expiry and Cancellation
- CI and Release Workflows
- Worker Test Lifecycle
- Content Hashing
- Service Installation
- Fake Llama Backend
- Worker Artifact Store
- Worker Message Handling
- Worker CLI
- Catalog Database
- Server-Sent Events
- Model and Workflow Versions
- Lease Scheduling Conversion
- Database Authentication
- Credential Storage
- OpenAI Network Security
- Backend Abstractions
- Artifact Manifest
- Remote Configuration
- Scheduler Assignment
- Local Artifact Store
- Core Domain Entities
- Queue Scheduling
- Database Test Fixtures
- Authentication
- Domain IDs and Modalities
- Failure Settlement
- Worker Enrollment
- Image Input Resolution
- Scheduler Candidates
- Execution Context
- Catalog Row Decoding
- API Error Responses
- HTTP Authentication
- Scheduler Wakeups
- Worker Service Protocol
- E2E Admin CLI
- Catalog Database Rows
- Generation Test Helpers
- Image URL Parsing
- Model Listing API
- PostgreSQL Intervals
- Generation Targets
- Telemetry
- Health Endpoints
- PostgreSQL Notifications
- Build Script
- Worker Runtime Model
- Rust Crate Architecture
- Remote Entry Point
- Artifact Concepts
- Model Concepts
- Workflow Concepts
- Artifact Storage
- Remote Session Script

## God Nodes (most connected - your core abstractions)
1. `AppState` - 116 edges
2. `ContentHash` - 86 edges
3. `Harness` - 42 edges
4. `BackendError` - 41 edges
5. `ArtifactManifest` - 35 edges
6. `ArtifactRow` - 34 edges
7. `FailureKind` - 32 edges
8. `ApiError` - 29 edges
9. `ComfyBackend` - 29 edges
10. `ExecutionContext` - 28 edges

## Surprising Connections (you probably didn't know these)
- `gpq-remote coordinator` --conceptually_related_to--> `Tenant`  [INFERRED]
  README.md → CONTEXT.md
- `Tenant administration` --references--> `Tenant Master Key`  [INFERRED]
  README.md → CONTEXT.md
- `Leased at-least-once execution` --conceptually_related_to--> `Attempt`  [INFERRED]
  README.md → CONTEXT.md
- `gpq-worker service` --conceptually_related_to--> `Worker`  [INFERRED]
  README.md → CONTEXT.md
- `Worker configuration` --references--> `Device Pool`  [INFERRED]
  README.md → CONTEXT.md

## Import Cycles
- 2-file cycle: `crates/gpq-remote/src/artifacts.rs -> crates/gpq-remote/src/state.rs -> crates/gpq-remote/src/artifacts.rs`
- 2-file cycle: `crates/gpq-remote/src/scheduler.rs -> crates/gpq-remote/src/state.rs -> crates/gpq-remote/src/scheduler.rs`
- 3-file cycle: `crates/gpq-remote/src/artifacts.rs -> crates/gpq-remote/src/http.rs -> crates/gpq-remote/src/state.rs -> crates/gpq-remote/src/artifacts.rs`

## Hyperedges (group relationships)
- **Tenant isolation and operations** — docs_adr_0001_tenant_dedicated_workers_keep_workers_tenant_dedicated, docs_adr_0009_separate_tenant_and_worker_credentials_separate_tenant_and_worker_credentials, docs_adr_0011_trust_remote_and_enforce_rls_trust_remote_and_enforce_database_tenant_isolation, docs_adr_0016_separate_migration_from_serving_separate_migration_from_serving, docs_adr_0018_do_not_sandbox_custom_nodes_do_not_sandbox_comfyui_custom_nodes [INFERRED 0.85]
- **Generation execution and delivery** — docs_adr_0002_prioritize_gpu_utilization_prioritize_gpu_utilization, docs_adr_0003_use_leased_at_least_once_execution_use_leased_at_least_once_execution, docs_adr_0007_unify_generation_lifecycle_only_unify_the_generation_lifecycle_only, docs_adr_0008_keep_records_but_expire_artifacts_keep_records_but_expire_artifacts [INFERRED 0.85]
- **Tenant generation worker execution flow** — context_tenant, context_generation, context_attempt, context_worker, context_accepted_result [EXTRACTED 1.00]
- **CI quality gate** — github_workflows_test_protobuf_lint, github_workflows_test_cargo_check, github_workflows_test_unit_tests, github_workflows_test_clippy [EXTRACTED 1.00]

## Communities (86 total, 7 thin omitted)

### Community 0 - "E2E ComfyUI Tests"
Cohesion: 0.05
Nodes (93): build_fixtures(), fake(), Fixtures, harness(), interrupt_cancels_running_image_generation(), object_store(), object_store_output_placement_lands_in_bucket_and_redirects(), register_workflow() (+85 more)

### Community 1 - "Artifact Service"
Cohesion: 0.06
Nodes (70): Bytes, ArtifactService, as_u64(), checked_grow(), ChunkDrainOutcome, classify_download(), deliver_request(), deliver_worker_local() (+62 more)

### Community 2 - "Capability Admission Tests"
Cohesion: 0.07
Nodes (72): BTreeSet, accelerator_memory_boundary_is_inclusive(), extra_models_and_custom_nodes_do_not_block_admission(), hash(), IncapableReason, missing_custom_node_reports_no_installed_version(), oom_marking_removes_the_slot_from_candidates(), rejects_other_tenants() (+64 more)

### Community 3 - "Remote E2E Support"
Cohesion: 0.06
Nodes (55): CatalogServiceClient, ClientConfig, AdminCli, chat_parameters(), collect_sse_json(), database_url_for(), database_url_with_credentials(), enroll_worker() (+47 more)

### Community 4 - "Worker Configuration"
Cohesion: 0.06
Nodes (69): CacheFile, check_device_overlap(), duration_secs(), ensure_state_dir(), parses_full_sample(), PoolConfig, rejects_duplicate_pool_keys(), rejects_empty_pools() (+61 more)

### Community 5 - "Priority Value Object"
Cohesion: 0.07
Nodes (64): i16, Priority, PriorityOutOfRange, Default, Display, Error, Formatter, From (+56 more)

### Community 6 - "Native Generation API"
Cohesion: 0.05
Nodes (64): ArtifactRef, CancelGenerationRequest, CancelGenerationResponse, admission_error(), artifact_row_to_proto(), discontinuity_event(), discontinuity_event_reports_the_given_reason(), domain_event_to_proto() (+56 more)

### Community 7 - "Llama Backend API"
Cohesion: 0.07
Nodes (61): ChatMessage, build_chat_request(), build_chat_request_forces_stream_and_passes_through_fields(), build_chat_request_injects_seed_overriding_existing(), build_chat_request_omits_seed_when_absent(), build_chat_request_rejects_non_object_parameters(), cancelled_error(), ChatChunk (+53 more)

### Community 8 - "Worker Session"
Cohesion: 0.06
Nodes (62): AbortOnDrop, accept_lease(), backoff_base(), cancel_all_attempts(), cancel_attempt(), cancel_expired_leases(), capability_report_message(), classify_lease() (+54 more)

### Community 9 - "Tenant Administration"
Cohesion: 0.07
Nodes (61): overdue_is_inclusive_of_the_boundary(), Default, Duration, Self, TenantSettings, create_tenant(), decode_error(), delete_tenant() (+53 more)

### Community 10 - "Domain State Machines"
Cohesion: 0.07
Nodes (46): ArtifactPlacement, ArtifactState, Result, Self, terminal_attempt_states_are_final(), TransitionError, ArtifactDirection, ArtifactRow (+38 more)

### Community 11 - "PostgreSQL Integration Tests"
Cohesion: 0.11
Nodes (60): accept_result_rejects_a_result_for_a_terminal_generation(), accept_result_rejects_a_result_under_an_expired_lease(), accept_result_rejects_a_second_result_once_accepted(), accept_result_settles_the_first_result_and_releases_the_slot(), active_tenants_excludes_soft_deleted_tenants(), artifact_lifecycle_available_to_delivering_to_consumed(), artifact_output_expires_one_hour_after_completion(), artifact_second_delivery_attempt_loses_the_conflict() (+52 more)

### Community 12 - "OpenAI Chat API"
Cohesion: 0.07
Nodes (57): GenerationState, chat_completions(), chat_finish_reason(), ChatChoice, ChatCompletionChunk, ChatCompletionRequest, ChatCompletionResponse, ChatMessage (+49 more)

### Community 13 - "Process Management"
Cohesion: 0.08
Nodes (49): ChildStderr, ChildStdout, command_line(), command_line_corroborates(), command_line_corroboration_accepts_interpreter_launched_backends(), create_and_assign(), decide(), filetime_to_u64() (+41 more)

### Community 14 - "Worker Pool Scheduling"
Cohesion: 0.07
Nodes (41): Backend, Send, Sync, acquire_fails_on_an_unready_pool(), acquire_never_exceeds_capacity(), exit_detection_marks_pool_unready_and_clears_capacity(), lock_state(), mark_unready_for_exit() (+33 more)

### Community 15 - "Execution Lifecycle"
Cohesion: 0.08
Nodes (46): FailureKind, download_input(), execute(), execution_timeout_duration(), ExecutionOutcome, failure_kind_to_proto(), fetch_inputs(), handle_event() (+38 more)

### Community 16 - "Fake ComfyUI Backend"
Cohesion: 0.09
Nodes (47): base_graph(), checkpoint_name(), event(), FakeComfy, free(), handle_socket(), hang_graph(), has_node_class() (+39 more)

### Community 17 - "Retry and Lease Logic"
Cohesion: 0.10
Nodes (40): RetryDecision, lease_expires_forty_five_seconds_after_now(), lease_expiry_from(), DateTime, Utc, AttemptState, acknowledge_cancel(), AttemptRow (+32 more)

### Community 18 - "Event Persistence"
Cohesion: 0.08
Nodes (48): append(), append_attempt_created(), EventKind, EventRow, latest(), load_since(), AttemptId, DateTime (+40 more)

### Community 19 - "ComfyUI Backend"
Cohesion: 0.06
Nodes (36): apply_override(), apply_parameters(), apply_parameters_overrides_addressed_node_input(), apply_parameters_places_seed_at_reserved_pointer(), apply_parameters_rejects_malformed_pointer(), apply_parameters_rejects_unknown_node_id(), apply_parameters_skips_seed_without_pointer(), checkpoint_name() (+28 more)

### Community 20 - "Catalog Service"
Cohesion: 0.08
Nodes (42): CatalogService, any_candidate_remains(), IntoIterator, Item, catalog_error(), CatalogApi, custom_node_without_exact_version_is_rejected(), internal() (+34 more)

### Community 21 - "OpenAI Responses API"
Cohesion: 0.08
Nodes (47): build_response_object(), collect_input_artifacts(), completed_response_carries_output_text_and_usage(), CompletedEvent, create_response(), CreatedEvent, CreateResponseRequest, DeltaEvent (+39 more)

### Community 22 - "Artifact Transfer"
Cohesion: 0.07
Nodes (40): ArtifactChunk, a_buffer_exactly_a_multiple_of_the_chunk_size_ends_on_a_boundary(), a_single_chunk_fitting_exactly_is_marked_last(), build_chunks(), ChunkReceipt, chunks_cover_the_whole_buffer_from_zero(), DeliveryValidation, domain_manifest() (+32 more)

### Community 23 - "Worker Session Protocol"
Cohesion: 0.07
Nodes (41): AttemptRunning, CancelAcknowledged, accepted_outcome_carries_the_generation_id_and_is_not_discarded(), classify_result_outcome(), domain_backend_kind(), domain_failure_kind(), domain_manifest_from_proto(), domain_placement() (+33 more)

### Community 24 - "Native Protobuf Conversion"
Cohesion: 0.07
Nodes (41): artifact_placement_from_proto(), artifact_placement_round_trips_through_proto(), artifact_placement_to_proto(), artifact_state_to_proto(), authenticate(), backend_kind_to_proto(), duration_from_proto(), duration_round_trips_through_proto() (+33 more)

### Community 25 - "Remote CLI"
Cohesion: 0.09
Nodes (37): Cli, Command, CancellationToken, Command, Option, Result, String, Uuid (+29 more)

### Community 26 - "ComfyUI Execution"
Cohesion: 0.14
Nodes (23): ComfyBackend, custom_node_versions(), custom_node_versions_falls_back_to_unknown(), custom_node_versions_prefers_known_package_version(), extract_output_entries(), internal_error(), ObjectInfoEntry, probe_ok() (+15 more)

### Community 27 - "Generation Admission"
Cohesion: 0.13
Nodes (36): a_model_and_a_workflow_alias_of_the_same_name_hash_differently(), AdmissionError, AdmissionRequest, admit(), AliasTarget, differing_parameters_hash_differently(), differing_seed_hashes_differently(), ensure_synchronous_capacity() (+28 more)

### Community 28 - "Architecture Decisions"
Cohesion: 0.06
Nodes (40): Keep workers tenant-dedicated, tenant trust boundaries, cache-aware GPU scheduling, Prioritize GPU utilization, leased at-least-once delivery, Use leased at-least-once execution, public and worker transports, Separate public and Worker transports (+32 more)

### Community 29 - "Expiry and Cancellation"
Cohesion: 0.13
Nodes (34): cancel_request_carries_the_attempt_id_and_reason(), cancel_request_message(), cancel_synchronous_for_tenant(), cancel_synchronous_on_startup(), discard_input_artifacts(), enforce_execution_deadlines(), expire_artifacts(), expire_leases() (+26 more)

### Community 30 - "CI and Release Workflows"
Cohesion: 0.07
Nodes (36): Buf breaking check, Buf configuration, Buf lint, proto module, STANDARD lint rules, Build and release job, Docker Buildx, Docker image build and push (+28 more)

### Community 31 - "Worker Test Lifecycle"
Cohesion: 0.12
Nodes (25): enroll(), open_session(), random_content_sha256(), CancelRequest, ConnectError, DiscardOutput, Duration, Harness (+17 more)

### Community 32 - "Content Hashing"
Cohesion: 0.10
Nodes (18): ContentHash, digest_matches_known_vector(), Hasher, round_trips_through_hex(), D, Display, Err, Error (+10 more)

### Community 33 - "Service Installation"
Cohesion: 0.18
Nodes (31): install(), install_linux(), install_macos(), install_windows(), launchd_plist_path(), launchd_plist_wires_program_arguments(), quote_systemd_word(), quote_windows_arg() (+23 more)

### Community 34 - "Fake Llama Backend"
Cohesion: 0.13
Nodes (26): chat_completions(), FakeLlama, FakeMode, health(), lock(), props(), Arc, Duration (+18 more)

### Community 35 - "Worker Artifact Store"
Cohesion: 0.20
Nodes (25): ArtifactChunkData, ArtifactReader, delete_is_idempotent(), expire_removes_only_artifacts_past_ttl(), manifest(), open_for_read_rejects_an_offset_past_the_manifest(), open_store(), publish_rejects_digest_mismatch_and_leaves_no_directory() (+17 more)

### Community 36 - "Worker Message Handling"
Cohesion: 0.16
Nodes (29): AttemptFailure, AttemptProgress, AttemptResult, AttemptTokenDelta, CapabilityReport, handle_capability_report(), handle_heartbeat(), load_tenant_settings() (+21 more)

### Community 37 - "Worker CLI"
Cohesion: 0.15
Nodes (28): Cli, combine_uninstall_results(), combine_uninstall_results_ok_when_both_succeed(), combine_uninstall_results_reports_both_failures_when_neither_step_succeeds(), combine_uninstall_results_surfaces_credential_failure_alone(), combine_uninstall_results_surfaces_service_failure_alone(), Command, ConfigArgs (+20 more)

### Community 38 - "Catalog Database"
Cohesion: 0.23
Nodes (26): CatalogError, content_hash_changes_with_graph(), content_hash_ignores_limits(), content_hash_is_stable_under_key_reordering(), delete_model_alias(), delete_workflow_alias(), get_model_version(), get_workflow_version_row() (+18 more)

### Community 39 - "Server-Sent Events"
Cohesion: 0.17
Nodes (24): advance(), BroadcastFrames, CancelOnDrop, data_event(), done_event(), finish_with(), named_event(), Drop (+16 more)

### Community 40 - "Model and Workflow Versions"
Cohesion: 0.13
Nodes (22): Modality, ExecutionLimits, ModelVersion, resolve_execution_timeout(), BTreeMap, Duration, MediaKind, Option (+14 more)

### Community 41 - "Lease Scheduling Conversion"
Cohesion: 0.12
Nodes (22): build_lease_assignment(), duration_from_micros_round_trips(), duration_from_negative_micros_falls_back_to_zero(), lease_target_fields(), leased_output_key(), model_target_leaves_workflow_fields_unset(), proto_artifact_manifest(), proto_artifact_placement() (+14 more)

### Community 42 - "Database Authentication"
Cohesion: 0.13
Nodes (14): KeyedHasher, Vec, Db, DateTime, Option, PgPool, Postgres, Result (+6 more)

### Community 43 - "Credential Storage"
Cohesion: 0.20
Nodes (14): accepts_owner_only_file(), CredentialStore, current_uid(), load_unix_file_credential(), missing_file_is_none(), rejects_group_readable_file(), rejects_world_readable_file(), Option (+6 more)

### Community 44 - "OpenAI Network Security"
Cohesion: 0.11
Nodes (13): ip_literal_host_public_address_is_accepted(), is_publicly_routable(), is_publicly_routable_v4(), is_publicly_routable_v6(), resolve_safe_addrs(), SocketAddr, TenantAuth, usage_from_row() (+5 more)

### Community 45 - "Backend Abstractions"
Cohesion: 0.16
Nodes (21): BackendCapabilities, build(), classify_transport_error(), client_error_status_normalizes_to_internal(), connect_failure_normalizes_to_backend_crashed(), ExecutionRequest, InputArtifact, normalize_transport_error() (+13 more)

### Community 46 - "Artifact Manifest"
Cohesion: 0.13
Nodes (12): ArtifactManifest, manifest(), ManifestMismatch, MediaKind, Result, String, verification_accepts_matching_bytes(), verification_rejects_short_transfer() (+4 more)

### Community 47 - "Remote Configuration"
Cohesion: 0.19
Nodes (15): credential_key_parses_valid_hex(), object_store_absent_is_none(), object_store_default_presign_ttl_is_fifteen_minutes(), object_store_full_configuration_resolves(), ObjectStoreConfig, parse_credential_key(), RemoteConfig, resolve_object_store() (+7 more)

### Community 48 - "Scheduler Assignment"
Cohesion: 0.19
Nodes (19): assign(), eligible_pool_is_bounded_by_its_advertised_free_slots(), known_tenants(), plan_assignments(), pool_whose_worker_has_no_live_session_is_skipped(), pool_with_no_free_slots_is_skipped(), DateTime, DevicePoolId (+11 more)

### Community 49 - "Local Artifact Store"
Cohesion: 0.20
Nodes (12): ArtifactHandle, LocalArtifactStore, ReconcileReport, ArtifactId, AttemptId, DateTime, Duration, PathBuf (+4 more)

### Community 50 - "Core Domain Entities"
Cohesion: 0.13
Nodes (18): Accepted Result, Attempt, Generation, Tenant, Tenant Master Key, Worker, Worker Credential, ComfyUI (+10 more)

### Community 51 - "Queue Scheduling"
Cohesion: 0.24
Nodes (14): age_breaks_ties_within_equal_priority(), batches_share_the_pinned_version_and_respect_capacity(), empty_queue_yields_no_batch(), Fixture, hash(), incompatible_work_is_excluded(), no_free_slot_means_no_work(), overdue_selection_ignores_priority() (+6 more)

### Community 52 - "Database Test Fixtures"
Cohesion: 0.34
Nodes (17): attempt_rows(), AttemptRow, db_now(), event_kinds(), generation_row(), generation_row_created_after(), GenerationRow, latest_generation_row() (+9 more)

### Community 53 - "Authentication"
Cohesion: 0.18
Nodes (15): bearer_token(), bearer_token_is_none_without_a_header(), bearer_token_parses_case_insensitively(), bearer_token_parses_the_standard_scheme(), bearer_token_rejects_other_schemes(), different_secrets_hash_differently(), generate_secret(), generated_secrets_carry_their_prefix_and_are_unique() (+7 more)

### Community 54 - "Domain IDs and Modalities"
Cohesion: 0.13
Nodes (5): assert_serde_matches(), T, serde_names_match_stable_names(), Duration, Failure

### Community 55 - "Failure Settlement"
Cohesion: 0.17
Nodes (15): internal(), not_renewed_attempts_each_get_a_cancel_request(), not_renewed_cancel_messages(), only_worker_local_placement_attributes_an_owning_worker(), reject_oversized_outputs(), AttemptId, AttemptOutput, ConnectError (+7 more)

### Community 56 - "Worker Enrollment"
Cohesion: 0.20
Nodes (12): EnrollmentApi, internal(), internal_anyhow(), ConnectError, EnrollResponse, Error, RequestContext, Self (+4 more)

### Community 57 - "Image Input Resolution"
Cohesion: 0.22
Nodes (11): build_admission_request(), data_url_enforces_size_cap(), InputArtifactGuard, resolve_and_store_images(), ArtifactId, Drop, IntoIterator, Item (+3 more)

### Community 58 - "Scheduler Candidates"
Cohesion: 0.29
Nodes (10): Candidate, DateTime, Duration, GenerationId, Option, Utc, Vec, select_batch() (+2 more)

### Community 59 - "Execution Context"
Cohesion: 0.14
Nodes (13): DirGuard, ExecutionContext, Arc, AttemptId, CancellationToken, Client, Drop, LeaseAssignment (+5 more)

### Community 60 - "Catalog Row Decoding"
Cohesion: 0.41
Nodes (10): decode_error(), decode_model_execution_limits(), decode_workflow_manifest(), parse_content_hash(), parse_modality(), resolve_model_alias(), resolve_workflow_alias(), Display (+2 more)

### Community 61 - "API Error Responses"
Cohesion: 0.20
Nodes (10): ApiError, ErrorBody, ErrorDetail, failure_status_and_code(), Option, Response, StatusCode, terminal_failure_response() (+2 more)

### Community 62 - "HTTP Authentication"
Cohesion: 0.26
Nodes (6): ipv6_mapped_private_is_rejected(), router(), Into, Self, Parts, Rejection

### Community 63 - "Scheduler Wakeups"
Cohesion: 0.20
Nodes (9): JoinHandle, Receiver, Self, Sender, WorkerId, run(), SchedulerHandle, spawn() (+1 more)

### Community 64 - "Worker Service Protocol"
Cohesion: 0.17
Nodes (11): log_on_err(), E, Encodable, Future, Output, RequestContext, Send, ServiceResult (+3 more)

### Community 65 - "E2E Admin CLI"
Cohesion: 0.42
Nodes (11): migrate(), RotatedKey, Output, Path, Result, String, Uuid, run() (+3 more)

### Community 66 - "Catalog Database Rows"
Cohesion: 0.33
Nodes (11): ModelAliasRow, ModelVersionRawRow, ResolvedModelRawRow, ResolvedWorkflowRawRow, DateTime, PgInterval, String, Utc (+3 more)

### Community 67 - "Generation Test Helpers"
Cohesion: 0.29
Nodes (11): await_terminal_generation(), fetch_generation_row(), media_kind_from_mime(), GenerationId, GenerationRow, MediaKind, Receiver, Result (+3 more)

### Community 68 - "Image URL Parsing"
Cohesion: 0.33
Nodes (9): data_url_decodes_base64(), data_url_decodes_percent_encoded_text(), fetch_http_image(), parse_data_url(), resolve_image_input(), ResolvedImage, Duration, String (+1 more)

### Community 69 - "Model Listing API"
Cohesion: 0.29
Nodes (9): list_models(), ModelListResponse, ModelObject, Json, Result, State, String, Vec (+1 more)

### Community 70 - "PostgreSQL Intervals"
Cohesion: 0.39
Nodes (8): interval_days_and_microseconds_combine(), interval_days_are_folded_into_a_fixed_day(), interval_months_fold_into_thirty_day_periods(), interval_round_trips_seconds(), interval_to_duration(), Duration, Error, PgInterval

### Community 72 - "Telemetry"
Cohesion: 0.38
Nodes (5): init(), Drop, Result, TelemetryGuard, SdkTracerProvider

### Community 73 - "Health Endpoints"
Cohesion: 0.53
Nodes (5): healthz(), readyz(), router(), State, StatusCode

### Community 74 - "PostgreSQL Notifications"
Cohesion: 0.40
Nodes (6): connect_listener(), recv_or_pending(), Error, Option, PgListener, PgNotification

### Community 75 - "Build Script"
Cohesion: 0.40
Nodes (4): main(), Box, Error, Result

### Community 76 - "Worker Runtime Model"
Cohesion: 0.50
Nodes (4): Active Runtime, Device Pool, Execution Slot, Worker configuration

### Community 77 - "Rust Crate Architecture"
Cohesion: 0.67
Nodes (4): gpq-domain, gpq-proto, gpq-remote, gpq-worker

## Knowledge Gaps
- **58 isolated node(s):** `UsageJson`, `DeltaEvent`, `remote_session_start.sh script`, `tenant trust boundaries`, `cache-aware GPU scheduling` (+53 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **7 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `AppState` connect `Expiry and Cancellation` to `Artifact Service`, `Native Generation API`, `Tenant Administration`, `OpenAI Chat API`, `Event Persistence`, `Catalog Service`, `OpenAI Responses API`, `Artifact Transfer`, `Worker Session Protocol`, `Native Protobuf Conversion`, `Remote CLI`, `Generation Admission`, `Worker Message Handling`, `Server-Sent Events`, `Lease Scheduling Conversion`, `Database Authentication`, `OpenAI Network Security`, `Remote Configuration`, `Scheduler Assignment`, `Failure Settlement`, `Worker Enrollment`, `Image Input Resolution`, `HTTP Authentication`, `Scheduler Wakeups`, `Worker Service Protocol`, `Generation Test Helpers`, `Model Listing API`, `Health Endpoints`, `PostgreSQL Notifications`?**
  _High betweenness centrality (0.305) - this node is a cross-community bridge._
- **Why does `ContentHash` connect `Content Hashing` to `Capability Admission Tests`, `Worker Configuration`, `Priority Value Object`, `Llama Backend API`, `Worker Pool Scheduling`, `Execution Lifecycle`, `Artifact Transfer`, `ComfyUI Execution`, `Catalog Database`, `Model and Workflow Versions`, `Backend Abstractions`, `Artifact Manifest`, `Scheduler Assignment`, `Queue Scheduling`, `Scheduler Candidates`, `Catalog Row Decoding`, `Catalog Database Rows`, `Model Listing API`, `Generation Targets`?**
  _High betweenness centrality (0.175) - this node is a cross-community bridge._
- **Why does `FailureKind` connect `Execution Lifecycle` to `Priority Value Object`, `Model and Workflow Versions`, `Worker Session`, `Backend Abstractions`, `Retry and Lease Logic`, `Event Persistence`, `ComfyUI Backend`, `Failure Settlement`, `Worker Session Protocol`, `Native Protobuf Conversion`, `ComfyUI Execution`, `API Error Responses`?**
  _High betweenness centrality (0.096) - this node is a cross-community bridge._
- **What connects `UsageJson`, `DeltaEvent`, `remote_session_start.sh script` to the rest of the system?**
  _58 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `E2E ComfyUI Tests` be split into smaller, more focused modules?**
  _Cohesion score 0.0502970297029703 - nodes in this community are weakly interconnected._
- **Should `Artifact Service` be split into smaller, more focused modules?**
  _Cohesion score 0.06153846153846154 - nodes in this community are weakly interconnected._
- **Should `Capability Admission Tests` be split into smaller, more focused modules?**
  _Cohesion score 0.06722689075630252 - nodes in this community are weakly interconnected._