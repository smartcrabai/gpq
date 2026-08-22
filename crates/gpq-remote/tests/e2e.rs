//! End-to-end tests driving the real `gpq-remote` and `gpq-worker` binaries
//! over their real wire protocols (Connect, gRPC, and plain HTTP/SSE).
//!
//! Every test shares one `testcontainers`-managed `PostgreSQL` 18 container
//! for this binary; `e2e_support::Harness` starts it lazily on first use, no
//! external `PostgreSQL` or Docker setup is required, and it is torn down by
//! `zzz_teardown_harness` — named to sort last so
//! `cargo test -p gpq-remote --test e2e -- --test-threads=1` runs it after
//! every other test regardless of pass/fail.
//!
//! Each test's doc comment names the ADR invariant it defends.

#[expect(
    dead_code,
    reason = "shared harness compiled into every integration-test binary; each suite \
              uses the subset it needs"
)]
mod e2e_support;

use std::time::Duration;

use anyhow::Context;
use e2e_support::fake_llama::FakeMode;
use e2e_support::{Harness, harness, wait_until};
use gpq_proto::gpq::v1 as pb;
use serde_json::json;

/// `GET /healthz` and `GET /readyz` answer `200` with no object storage
/// configured (ADR 0008: S3 is optional and never affects readiness).
#[test]
fn healthz_and_readyz_ok_without_object_storage() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let healthz = h.http.get(h.url("/healthz")).send().await?;
        anyhow::ensure!(
            healthz.status().is_success(),
            "healthz: {}",
            healthz.status()
        );

        let readyz = h.http.get(h.url("/readyz")).send().await?;
        anyhow::ensure!(readyz.status().is_success(), "readyz: {}", readyz.status());
        Ok(())
    })
}

/// `GET /v1/models` requires a valid Tenant Master Key (`401
/// invalid_api_key` otherwise) and lists Model aliases only, never Workflow
/// aliases or Model Version detail (ADR 0006, ADR 0012).
#[test]
fn list_models_requires_master_key_and_lists_alias_only() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let unauthenticated = h.http.get(h.url("/v1/models")).send().await?;
        anyhow::ensure!(
            unauthenticated.status() == reqwest::StatusCode::UNAUTHORIZED,
            "expected 401, got {}",
            unauthenticated.status()
        );
        let body: serde_json::Value = unauthenticated.json().await?;
        anyhow::ensure!(
            body["error"]["code"] == "invalid_api_key",
            "unexpected error body: {body}"
        );

        let authenticated = h
            .http
            .get(h.url("/v1/models"))
            .bearer_auth(&h.tenant1.master_key)
            .send()
            .await?;
        anyhow::ensure!(authenticated.status().is_success());
        let body: serde_json::Value = authenticated.json().await?;
        let data = body["data"]
            .as_array()
            .context("models response missing `data` array")?;
        anyhow::ensure!(
            data.iter().all(|entry| entry["object"] == "model"),
            "a non-model object leaked into /v1/models: {data:?}"
        );
        let ids: Vec<&str> = data
            .iter()
            .filter_map(|entry| entry["id"].as_str())
            .collect();
        anyhow::ensure!(
            ids.contains(&h.model_alias.as_str()),
            "expected {} among {ids:?}",
            h.model_alias
        );
        Ok(())
    })
}

/// A non-streaming `POST /v1/chat/completions` returns a `chat.completion`
/// carrying the fake backend's text and usage, and the underlying
/// Generation settles `succeeded` with exactly one Attempt whose
/// `attempt_created`/`state_changed` events are persisted (ADR 0003, ADR
/// 0008).
#[test]
fn chat_completion_succeeds_with_one_attempt_and_persists_events() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        h.fake
            .set_mode(FakeMode::reply("hello from the fake backend"));

        let response = h
            .http
            .post(h.url("/v1/chat/completions"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&json!({
                "model": h.model_alias,
                "messages": [{"role": "user", "content": "say hi"}],
                "stream": false,
            }))
            .send()
            .await?;
        anyhow::ensure!(
            response.status().is_success(),
            "status {}",
            response.status()
        );
        let body: serde_json::Value = response.json().await?;
        anyhow::ensure!(body["object"] == "chat.completion", "body: {body}");
        anyhow::ensure!(
            body["choices"][0]["message"]["content"] == "hello from the fake backend",
            "body: {body}"
        );
        let usage = &body["usage"];
        anyhow::ensure!(
            usage["total_tokens"].as_u64().unwrap_or(0) > 0,
            "expected nonzero usage, got {usage}"
        );

        let generation_id = body["id"]
            .as_str()
            .and_then(|id| id.strip_prefix("chatcmpl-"))
            .context("completion id missing chatcmpl- prefix")?;
        let uuid = uuid::Uuid::parse_str(generation_id)?;

        let row = h
            .generation_row(h.tenant1.id, uuid)
            .await?
            .context("generation row missing")?;
        anyhow::ensure!(row.state == "succeeded", "state: {}", row.state);
        anyhow::ensure!(
            row.attempt_count == 1,
            "attempt_count: {}",
            row.attempt_count
        );

        let attempts = h.attempt_rows(h.tenant1.id, uuid).await?;
        anyhow::ensure!(attempts.len() == 1, "attempts: {attempts:?}");
        anyhow::ensure!(
            attempts[0].state == "succeeded",
            "attempt state: {:?}",
            attempts[0]
        );

        let events = h.event_kinds(h.tenant1.id, uuid).await?;
        anyhow::ensure!(
            events.contains(&"attempt_created".to_owned()),
            "events: {events:?}"
        );
        anyhow::ensure!(
            events.contains(&"state_changed".to_owned()),
            "events: {events:?}"
        );
        Ok(())
    })
}

/// A streaming `POST /v1/chat/completions` emits a role chunk, content
/// chunks, a `finish_reason` chunk, a usage chunk when
/// `stream_options.include_usage` is set, and terminates with `data:
/// [DONE]` (ADR 0006).
#[test]
fn streaming_chat_completion_emits_expected_chunk_sequence() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        h.fake.set_mode(FakeMode::reply("streamed reply text"));

        let response = h
            .http
            .post(h.url("/v1/chat/completions"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&json!({
                "model": h.model_alias,
                "messages": [{"role": "user", "content": "stream please"}],
                "stream": true,
                "stream_options": {"include_usage": true},
            }))
            .send()
            .await?;
        anyhow::ensure!(
            response.status().is_success(),
            "status {}",
            response.status()
        );

        let events = e2e_support::collect_sse_json(response).await?;
        anyhow::ensure!(!events.is_empty(), "no SSE events received");

        let first_delta = &events[0]["choices"][0]["delta"];
        anyhow::ensure!(
            first_delta["role"] == "assistant",
            "first chunk: {:?}",
            events[0]
        );

        let content: String = events
            .iter()
            .filter_map(|event| event["choices"][0]["delta"]["content"].as_str())
            .collect();
        anyhow::ensure!(
            content == "streamed reply text",
            "assembled content: {content:?}"
        );

        let has_finish = events
            .iter()
            .any(|event| event["choices"][0]["finish_reason"] == "stop");
        anyhow::ensure!(has_finish, "no finish_reason chunk in {events:?}");

        let has_usage = events
            .iter()
            .any(|event| event["usage"]["total_tokens"].is_u64());
        anyhow::ensure!(has_usage, "no usage chunk in {events:?}");
        Ok(())
    })
}

/// An unknown Model alias yields `404 model_not_found`; a Model alias no
/// online Worker advertises yields `503 model_not_available` (ADR 0006).
#[test]
fn unknown_and_unavailable_aliases_return_openai_errors() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let unknown = h
            .http
            .post(h.url("/v1/chat/completions"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&json!({
                "model": "does-not-exist",
                "messages": [{"role": "user", "content": "hi"}],
            }))
            .send()
            .await?;
        anyhow::ensure!(
            unknown.status() == reqwest::StatusCode::NOT_FOUND,
            "status {}",
            unknown.status()
        );
        let body: serde_json::Value = unknown.json().await?;
        anyhow::ensure!(body["error"]["code"] == "model_not_found", "body: {body}");

        let unavailable_alias = h.register_unavailable_model_alias().await?;
        let unavailable = h
            .http
            .post(h.url("/v1/chat/completions"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&json!({
                "model": unavailable_alias,
                "messages": [{"role": "user", "content": "hi"}],
            }))
            .send()
            .await?;
        anyhow::ensure!(
            unavailable.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE,
            "status {}",
            unavailable.status()
        );
        let body: serde_json::Value = unavailable.json().await?;
        anyhow::ensure!(
            body["error"]["code"] == "model_not_available",
            "body: {body}"
        );
        Ok(())
    })
}

/// A backend HTTP 500 is classified `backend_crashed` and retried up to the
/// three-Attempt ceiling, after which the Generation fails (ADR 0003).
#[test]
fn backend_failure_exhausts_retries_and_fails_generation() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        h.fake.set_mode(FakeMode::Failing);

        let response = h
            .http
            .post(h.url("/v1/chat/completions"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&json!({
                "model": h.model_alias,
                "messages": [{"role": "user", "content": "this will fail"}],
                "stream": false,
            }))
            .send()
            .await?;
        anyhow::ensure!(
            response.status() == reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            "status {}",
            response.status()
        );
        let body: serde_json::Value = response.json().await?;
        anyhow::ensure!(body["error"]["code"] == "internal_error", "body: {body}");

        // The completion response carries no Generation id on failure, so
        // find the just-created Generation by its recency instead.
        let row = wait_until(
            || h.latest_generation_row(h.tenant1.id, "failed"),
            Duration::from_secs(15),
        )
        .await?;
        anyhow::ensure!(row.state == "failed", "state: {}", row.state);
        anyhow::ensure!(
            row.attempt_count == 3,
            "attempt_count: {}",
            row.attempt_count
        );

        let attempts = h.attempt_rows(h.tenant1.id, row.id).await?;
        anyhow::ensure!(attempts.len() == 3, "attempts: {attempts:?}");
        for (index, attempt) in attempts.iter().enumerate() {
            anyhow::ensure!(
                attempt.attempt_number == i32::try_from(index)? + 1,
                "attempt: {attempt:?}"
            );
            anyhow::ensure!(attempt.state == "failed", "attempt: {attempt:?}");
            anyhow::ensure!(
                attempt.failure_kind.as_deref() == Some("backend_crashed"),
                "attempt: {attempt:?}"
            );
        }

        h.fake.set_mode(FakeMode::reply("recovered"));
        Ok(())
    })
}

/// Native `Submit` (durable) reaches `succeeded` through `queued`/`running`;
/// a `Submit` targeting a Workflow alias with no `ComfyUI` Pool online
/// stays `queued` rather than being leased to the llama.cpp Pool (ADR 0002
/// capability filtering, ADR 0012 version pinning).
#[test]
fn native_submit_durable_lifecycle_and_workflow_alias_stays_queued() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        h.fake
            .set_mode(FakeMode::reply("native reply").with_delay(Duration::from_secs(2)));

        let generation = h.native_submit_model(&h.model_alias).await?;
        let generation_id = generation.generation_id.clone();

        let observed_nonterminal = wait_until(
            || async {
                let g = h.native_get_generation(&generation_id).await?;
                let nonterminal = g.state == pb::GenerationState::GENERATION_STATE_QUEUED
                    || g.state == pb::GenerationState::GENERATION_STATE_RUNNING;
                Ok(nonterminal.then_some(()))
            },
            Duration::from_secs(10),
        )
        .await;
        anyhow::ensure!(
            observed_nonterminal.is_ok(),
            "never observed queued/running"
        );

        let succeeded = wait_until(
            || async {
                let g = h.native_get_generation(&generation_id).await?;
                Ok((g.state == pb::GenerationState::GENERATION_STATE_SUCCEEDED).then_some(g))
            },
            Duration::from_secs(20),
        )
        .await?;
        anyhow::ensure!(
            succeeded.output_text == "native reply",
            "output: {}",
            succeeded.output_text
        );

        let workflow_alias = h.register_workflow_alias_without_worker().await?;
        let workflow_generation = h.native_submit_workflow(&workflow_alias).await?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        let still_queued = h
            .native_get_generation(&workflow_generation.generation_id)
            .await?;
        anyhow::ensure!(
            still_queued.state == pb::GenerationState::GENERATION_STATE_QUEUED,
            "expected queued, got {:?}",
            still_queued.state
        );
        Ok(())
    })
}

/// `WatchGeneration` over Connect streaming yields the current snapshot
/// first, then live events through the terminal `stateChanged` (ADR 0006).
#[test]
fn watch_generation_streams_snapshot_then_live_events() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        h.fake
            .set_mode(FakeMode::reply("watched reply").with_delay(Duration::from_millis(500)));

        let generation = h.native_submit_model(&h.model_alias).await?;
        let mut stream = h.native_watch_generation(&generation.generation_id).await?;

        let first = stream
            .message::<pb::GenerationEvent>()
            .await
            .map_err(|err| anyhow::anyhow!("watch stream error: {err}"))?
            .context("stream ended before any event")?
            .to_owned_message();
        anyhow::ensure!(
            matches!(first.event, Some(pb::generation_event::Event::Snapshot(_))),
            "first event was not a snapshot: {first:?}"
        );

        let mut saw_terminal = false;
        for _ in 0..64_u32 {
            let Some(item) = stream
                .message::<pb::GenerationEvent>()
                .await
                .map_err(|err| anyhow::anyhow!("watch stream error: {err}"))?
            else {
                break;
            };
            let event = item.to_owned_message();
            if let Some(pb::generation_event::Event::StateChanged(changed)) = event.event
                && changed.state == pb::GenerationState::GENERATION_STATE_SUCCEEDED
            {
                saw_terminal = true;
                break;
            }
        }
        anyhow::ensure!(saw_terminal, "never observed a terminal stateChanged event");
        Ok(())
    })
}

/// A client disconnecting over HTTP/2 mid-request moves the Generation to
/// `cancelling` and the expiry sweep re-sends `CancelRequest` until the
/// Worker acknowledges, settling `cancelled` (ADR 0003, ADR 0006).
#[test]
fn client_disconnect_cancels_generation_and_attempt() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        h.fake
            .set_mode(FakeMode::reply("too slow").with_delay(Duration::from_secs(20)));

        let marker = h.db_now().await?;

        let request = h
            .http2
            .post(h.url("/v1/chat/completions"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&json!({
                "model": h.model_alias,
                "messages": [{"role": "user", "content": "drop me"}],
                "stream": false,
            }))
            .send();
        // Give the request just long enough to be admitted and leased, then
        // abandon it: dropping the future tears down the HTTP/2 stream,
        // which `CancelOnDrop` observes as an immediate disconnect.
        let _ = tokio::time::timeout(Duration::from_millis(800), request).await;

        let row = wait_until(
            || h.generation_row_created_after(h.tenant1.id, marker),
            Duration::from_secs(5),
        )
        .await?;

        let cancelled = wait_until(
            || async {
                let row = h
                    .generation_row(h.tenant1.id, row.id)
                    .await?
                    .context("generation disappeared")?;
                Ok((row.state == "cancelled").then_some(row))
            },
            Duration::from_secs(30),
        )
        .await?;
        anyhow::ensure!(cancelled.state == "cancelled");

        let attempts = h.attempt_rows(h.tenant1.id, cancelled.id).await?;
        anyhow::ensure!(
            attempts.iter().any(|a| a.state == "cancelled"),
            "attempts: {attempts:?}"
        );

        h.fake.set_mode(FakeMode::reply("recovered"));
        Ok(())
    })
}

/// A second Tenant's Master Key cannot read the first Tenant's Generation
/// (`NotFound`) and sees no Models or Workers of its own (ADR 0001, ADR
/// 0011).
#[test]
fn cross_tenant_isolation_over_the_api() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        h.fake.set_mode(FakeMode::reply("tenant one's reply"));
        let generation = h.native_submit_model(&h.model_alias).await?;

        let tenant2_generation_client = h.generation_client(&h.tenant2.master_key);
        let request = pb::GetGenerationRequest {
            generation_id: generation.generation_id.clone(),
            ..Default::default()
        };
        let err = tenant2_generation_client
            .get_generation(request)
            .await
            .err()
            .context("tenant 2 unexpectedly read tenant 1's generation")?;
        anyhow::ensure!(
            err.code == connectrpc::ErrorCode::NotFound,
            "unexpected error code: {:?}",
            err.code
        );

        let tenant2_catalog_client = h.catalog_client(&h.tenant2.master_key);
        let models = tenant2_catalog_client
            .list_models(pb::ListModelsRequest::default())
            .await
            .map_err(|err| anyhow::anyhow!("tenant 2 list_models failed: {err}"))?
            .into_owned();
        anyhow::ensure!(
            models.aliases.is_empty(),
            "tenant 2 sees aliases: {:?}",
            models.aliases
        );

        let workers = tenant2_catalog_client
            .list_workers(pb::ListWorkersRequest::default())
            .await
            .map_err(|err| anyhow::anyhow!("tenant 2 list_workers failed: {err}"))?
            .into_owned();
        anyhow::ensure!(
            workers.workers.is_empty(),
            "tenant 2 sees workers: {:?}",
            workers.workers
        );
        Ok(())
    })
}

/// Killing the fake backend's managed process crashes it out from under the
/// Worker; the maintenance tick detects the exit, marks the Pool unready,
/// and restarts it with a fresh process, after which the Pool is ready
/// again (ADR 0005).
#[test]
fn backend_crash_recovery_restores_pool_readiness() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        h.fake.set_mode(FakeMode::reply("before the crash"));

        // Prove the Pool is ready and serving before killing anything.
        let warmup = h
            .http
            .post(h.url("/v1/chat/completions"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&json!({
                "model": h.model_alias,
                "messages": [{"role": "user", "content": "warmup"}],
                "stream": false,
            }))
            .send()
            .await?;
        anyhow::ensure!(
            warmup.status().is_success(),
            "warmup status {}",
            warmup.status()
        );

        let killed_pid = h.kill_managed_backend_process().await?;

        // The crash-detect-and-restart sequence runs inside a single
        // maintenance tick (every 30s), so the only reliably observable
        // proof of recovery is a *different* managed process, ready again
        // — not a transient `unready` row, which the same tick that
        // detects the crash also clears before ever reporting it.
        wait_until(
            || async {
                let current_pid = h.managed_backend_pid().await?;
                let ready = h.pool_is_ready(true).await?;
                Ok(match (current_pid, ready) {
                    (Some(pid), Some(())) if pid != killed_pid => Some(()),
                    _ => None,
                })
            },
            Duration::from_mins(1),
        )
        .await
        .context("worker never restarted the killed backend process into a ready pool")?;

        h.fake.set_mode(FakeMode::reply("after the recovery"));

        let response = h
            .http
            .post(h.url("/v1/chat/completions"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&json!({
                "model": h.model_alias,
                "messages": [{"role": "user", "content": "after recovery"}],
                "stream": false,
            }))
            .send()
            .await?;
        anyhow::ensure!(
            response.status().is_success(),
            "post-recovery status {}",
            response.status()
        );
        let body: serde_json::Value = response.json().await?;
        anyhow::ensure!(
            body["choices"][0]["message"]["content"] == "after the recovery",
            "body: {body}"
        );
        Ok(())
    })
}

/// Builds a `TenantService` client the same way `Harness::generation_client`
/// does internally, using only the crate's public dependencies and
/// `Harness::url` — `e2e_support` exposes no `tenant_client` accessor of its
/// own.
fn tenant_client(
    h: &Harness,
    master_key: &str,
) -> anyhow::Result<pb::TenantServiceClient<connectrpc::client::HttpClient>> {
    let uri: http::Uri = h
        .url("")
        .parse()
        .context("the harness base url must parse as a URI")?;
    let transport = connectrpc::client::HttpClient::plaintext_http2_only();
    let config = connectrpc::client::ClientConfig::new(uri)
        .with_protocol(connectrpc::Protocol::Connect)
        .with_codec_format(connectrpc::CodecFormat::Json)
        .with_default_header("authorization", format!("Bearer {master_key}"));
    Ok(pb::TenantServiceClient::new(transport, config))
}

/// Counts every nonterminal (queued, running, or cancelling) Generation for
/// `master_key`'s Tenant, paging through `ListGenerations` per state.
async fn count_nonterminal(h: &Harness, master_key: &str) -> anyhow::Result<u32> {
    let mut total = 0_u32;
    for state in [
        pb::GenerationState::GENERATION_STATE_QUEUED,
        pb::GenerationState::GENERATION_STATE_RUNNING,
        pb::GenerationState::GENERATION_STATE_CANCELLING,
    ] {
        let mut page_token = String::new();
        loop {
            let response = h
                .generation_client(master_key)
                .list_generations(pb::ListGenerationsRequest {
                    page_size: 200,
                    page_token: page_token.clone(),
                    state: state.into(),
                    ..Default::default()
                })
                .await
                .map_err(|err| anyhow::anyhow!("ListGenerations failed: {err}"))?
                .into_owned();
            total += u32::try_from(response.generations.len())?;
            if response.next_page_token.is_empty() {
                break;
            }
            page_token = response.next_page_token;
        }
    }
    Ok(total)
}

/// `GetTenantSettings` returns exactly the defaults `0001_initial.sql`'s
/// `tenants` table columns default to (ADR 0002, ADR 0006). Uses Tenant 2,
/// which no other test in this suite ever mutates, so this stays read-only.
#[test]
fn get_tenant_settings_returns_migration_defaults() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let response = tenant_client(h, &h.tenant2.master_key)?
            .get_tenant_settings(pb::GetTenantSettingsRequest::default())
            .await
            .map_err(|err| anyhow::anyhow!("GetTenantSettings failed: {err}"))?
            .into_owned();
        let settings = response
            .settings
            .into_option()
            .context("response missing settings")?;
        let queue_age = Duration::try_from(
            settings
                .maximum_queue_age
                .into_option()
                .context("settings missing maximum_queue_age")?,
        )
        .map_err(|err| anyhow::anyhow!("invalid maximum_queue_age: {err}"))?;
        anyhow::ensure!(
            queue_age == Duration::from_mins(30),
            "queue_age: {queue_age:?}"
        );
        anyhow::ensure!(
            settings.max_queued_generations == 1000,
            "max_queued_generations: {}",
            settings.max_queued_generations
        );
        anyhow::ensure!(
            settings.max_input_artifact_bytes == 256 * 1024 * 1024,
            "max_input_artifact_bytes: {}",
            settings.max_input_artifact_bytes
        );
        anyhow::ensure!(
            settings.max_output_artifact_bytes == 2 * 1024 * 1024 * 1024,
            "max_output_artifact_bytes: {}",
            settings.max_output_artifact_bytes
        );
        anyhow::ensure!(
            settings.default_priority == 5,
            "default_priority: {}",
            settings.default_priority
        );
        Ok(())
    })
}

/// `UpdateTenantSettings` changes only the fields named in `update_mask`,
/// leaving every other field at its prior value, and rejects a
/// `default_priority` outside `0..=9` (ADR 0006). Uses Tenant 2 and restores
/// its settings afterward so no later test observes the mutation.
#[test]
fn update_tenant_settings_honours_partial_mask_and_rejects_out_of_range() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let client = tenant_client(h, &h.tenant2.master_key)?;
        let original = client
            .get_tenant_settings(pb::GetTenantSettingsRequest::default())
            .await
            .map_err(|err| anyhow::anyhow!("GetTenantSettings failed: {err}"))?
            .into_owned()
            .settings
            .into_option()
            .context("response missing settings")?;

        let mut touched = original.clone();
        touched.default_priority = 7;
        let updated = client
            .update_tenant_settings(pb::UpdateTenantSettingsRequest {
                settings: touched.into(),
                update_mask: vec!["default_priority".to_owned()],
                ..Default::default()
            })
            .await
            .map_err(|err| anyhow::anyhow!("UpdateTenantSettings failed: {err}"))?
            .into_owned()
            .settings
            .into_option()
            .context("response missing settings")?;
        anyhow::ensure!(
            updated.default_priority == 7,
            "default_priority: {}",
            updated.default_priority
        );
        anyhow::ensure!(
            updated.max_queued_generations == original.max_queued_generations,
            "an unmasked field changed: {} != {}",
            updated.max_queued_generations,
            original.max_queued_generations
        );
        anyhow::ensure!(
            updated.max_input_artifact_bytes == original.max_input_artifact_bytes,
            "an unmasked field changed"
        );

        let mut out_of_range = updated.clone();
        out_of_range.default_priority = 10;
        let rejected = client
            .update_tenant_settings(pb::UpdateTenantSettingsRequest {
                settings: out_of_range.into(),
                update_mask: vec!["default_priority".to_owned()],
                ..Default::default()
            })
            .await;
        anyhow::ensure!(
            rejected.is_err(),
            "expected an out-of-range default_priority to be rejected"
        );

        client
            .update_tenant_settings(pb::UpdateTenantSettingsRequest {
                settings: original.into(),
                update_mask: Vec::new(),
                ..Default::default()
            })
            .await
            .map_err(|err| anyhow::anyhow!("restoring tenant settings failed: {err}"))?;
        Ok(())
    })
}

/// Lowering `max_queued_generations` to the Tenant's current nonterminal
/// count makes the very next admission fail `429 rate_limit_exceeded` on
/// the `OpenAI` surface, proving the setting actually reaches admission
/// (ADR 0002, ADR 0006) rather than being cosmetic.
#[test]
fn lowered_max_queued_generations_triggers_rate_limit_on_next_admission() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        h.fake
            .set_mode(FakeMode::reply("rate limit filler").with_delay(Duration::from_secs(10)));

        // A filler admission that stays nonterminal for the test's duration,
        // run on its own connection so it is never disconnected/cancelled.
        let filler_http = h.http.clone();
        let filler_url = h.url("/v1/chat/completions");
        let filler_key = h.tenant1.master_key.clone();
        let filler_model = h.model_alias.clone();
        let filler = tokio::spawn(async move {
            filler_http
                .post(filler_url)
                .bearer_auth(&filler_key)
                .json(&json!({
                    "model": filler_model,
                    "messages": [{"role": "user", "content": "filler"}],
                    "stream": false,
                }))
                .send()
                .await
        });

        let nonterminal = wait_until(
            || async {
                let count = count_nonterminal(h, &h.tenant1.master_key).await?;
                Ok((count >= 1).then_some(count))
            },
            Duration::from_secs(5),
        )
        .await
        .context("the filler admission never became nonterminal")?;

        let client = tenant_client(h, &h.tenant1.master_key)?;
        let original = client
            .get_tenant_settings(pb::GetTenantSettingsRequest::default())
            .await
            .map_err(|err| anyhow::anyhow!("GetTenantSettings failed: {err}"))?
            .into_owned()
            .settings
            .into_option()
            .context("response missing settings")?;
        let mut tightened = original.clone();
        tightened.max_queued_generations = nonterminal;
        client
            .update_tenant_settings(pb::UpdateTenantSettingsRequest {
                settings: tightened.into(),
                update_mask: vec!["max_queued_generations".to_owned()],
                ..Default::default()
            })
            .await
            .map_err(|err| anyhow::anyhow!("UpdateTenantSettings failed: {err}"))?;

        let response = h
            .http
            .post(h.url("/v1/chat/completions"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&json!({
                "model": h.model_alias,
                "messages": [{"role": "user", "content": "one too many"}],
                "stream": false,
            }))
            .send()
            .await?;
        let status = response.status();
        let body: serde_json::Value = response.json().await?;

        client
            .update_tenant_settings(pb::UpdateTenantSettingsRequest {
                settings: original.into(),
                update_mask: Vec::new(),
                ..Default::default()
            })
            .await
            .map_err(|err| anyhow::anyhow!("restoring tenant settings failed: {err}"))?;
        h.fake
            .set_mode(FakeMode::reply("hello from the fake backend"));
        let _ = filler.await;

        anyhow::ensure!(
            status == reqwest::StatusCode::TOO_MANY_REQUESTS,
            "status {status}"
        );
        anyhow::ensure!(
            body["error"]["code"] == "rate_limit_exceeded",
            "body: {body}"
        );
        Ok(())
    })
}

/// After `DeleteModelAlias`, `GET /v1/models` no longer lists that alias,
/// while a Generation admitted through it before deletion still runs to
/// completion: alias deletion never mutates Model Versions or past
/// Generations (ADR 0012).
#[test]
fn delete_model_alias_removes_listing_without_mutating_pinned_generations() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        h.fake.set_mode(
            FakeMode::reply("alias deletion reply").with_delay(Duration::from_millis(500)),
        );

        let models = h
            .catalog_client(&h.tenant1.master_key)
            .list_models(pb::ListModelsRequest::default())
            .await
            .map_err(|err| anyhow::anyhow!("ListModels failed: {err}"))?
            .into_owned();
        let existing = models
            .aliases
            .iter()
            .find(|alias| alias.alias == h.model_alias)
            .context("model_alias missing from ListModels")?;
        let content_sha256 = existing.content_sha256.clone();

        let temp_alias = format!("chat-model-temp-{}", &content_sha256[..8]);
        h.catalog_client(&h.tenant1.master_key)
            .set_model_alias(pb::SetModelAliasRequest {
                alias: temp_alias.clone(),
                content_sha256: content_sha256.clone(),
                ..Default::default()
            })
            .await
            .map_err(|err| anyhow::anyhow!("SetModelAlias failed: {err}"))?;

        let generation = h.native_submit_model(&temp_alias).await?;

        // Delete the alias while the Attempt is very likely still running
        // (the fake replies after 500ms): deletion must not disturb it.
        h.catalog_client(&h.tenant1.master_key)
            .delete_model_alias(pb::DeleteAliasRequest {
                alias: temp_alias.clone(),
                ..Default::default()
            })
            .await
            .map_err(|err| anyhow::anyhow!("DeleteModelAlias failed: {err}"))?;

        let response = h
            .http
            .get(h.url("/v1/models"))
            .bearer_auth(&h.tenant1.master_key)
            .send()
            .await?;
        let body: serde_json::Value = response.json().await?;
        let ids: Vec<&str> = body["data"]
            .as_array()
            .context("models response missing `data` array")?
            .iter()
            .filter_map(|entry| entry["id"].as_str())
            .collect();
        anyhow::ensure!(
            !ids.contains(&temp_alias.as_str()),
            "deleted alias still listed: {ids:?}"
        );
        anyhow::ensure!(
            ids.contains(&h.model_alias.as_str()),
            "unrelated alias {} vanished too: {ids:?}",
            h.model_alias
        );

        let succeeded = wait_until(
            || async {
                let g = h.native_get_generation(&generation.generation_id).await?;
                Ok((g.state == pb::GenerationState::GENERATION_STATE_SUCCEEDED).then_some(g))
            },
            Duration::from_secs(15),
        )
        .await
        .context("the generation pinned to the deleted alias never completed")?;
        anyhow::ensure!(
            succeeded.version_sha256 == content_sha256,
            "version_sha256 changed after alias deletion"
        );

        h.fake
            .set_mode(FakeMode::reply("hello from the fake backend"));
        Ok(())
    })
}

/// Non-streaming `POST /v1/responses` returns a `response` object carrying
/// `output` and `usage` (ADR 0006).
#[test]
fn responses_non_streaming_returns_response_object_with_usage() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        h.fake.set_mode(FakeMode::reply("hello from responses"));
        let response = h
            .http
            .post(h.url("/v1/responses"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&json!({"model": h.model_alias, "input": "say hi", "stream": false}))
            .send()
            .await?;
        anyhow::ensure!(
            response.status().is_success(),
            "status {}",
            response.status()
        );
        let body: serde_json::Value = response.json().await?;
        anyhow::ensure!(body["object"] == "response", "body: {body}");
        anyhow::ensure!(body["status"] == "completed", "body: {body}");
        anyhow::ensure!(
            body["output"][0]["content"][0]["text"] == "hello from responses",
            "body: {body}"
        );
        anyhow::ensure!(
            body["usage"]["total_tokens"].as_u64().unwrap_or(0) > 0,
            "usage: {}",
            body["usage"]
        );
        Ok(())
    })
}

/// Streaming `POST /v1/responses` emits `response.created`,
/// `response.output_text.delta`, and `response.completed`, ending with
/// `data: [DONE]` (ADR 0006).
#[test]
fn responses_streaming_emits_expected_event_sequence() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        h.fake.set_mode(FakeMode::reply("streamed response text"));
        let response = h
            .http
            .post(h.url("/v1/responses"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&json!({"model": h.model_alias, "input": "stream please", "stream": true}))
            .send()
            .await?;
        anyhow::ensure!(
            response.status().is_success(),
            "status {}",
            response.status()
        );

        let events = e2e_support::collect_sse_json(response).await?;
        anyhow::ensure!(!events.is_empty(), "no SSE events received");
        anyhow::ensure!(
            events[0]["type"] == "response.created",
            "first event: {:?}",
            events[0]
        );

        let deltas: String = events
            .iter()
            .filter(|event| event["type"] == "response.output_text.delta")
            .filter_map(|event| event["delta"].as_str())
            .collect();
        anyhow::ensure!(
            deltas == "streamed response text",
            "assembled deltas: {deltas:?}"
        );

        let completed = events.iter().any(|event| {
            event["type"] == "response.completed" && event["response"]["status"] == "completed"
        });
        anyhow::ensure!(completed, "no response.completed in {events:?}");
        Ok(())
    })
}

/// `previous_response_id` is rejected: GPQ keeps no cross-Generation
/// conversation state (ADR 0006).
#[test]
fn responses_rejects_previous_response_id() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let response = h
            .http
            .post(h.url("/v1/responses"))
            .bearer_auth(&h.tenant1.master_key)
            .json(
                &json!({"model": h.model_alias, "input": "hi", "previous_response_id": "resp_123"}),
            )
            .send()
            .await?;
        anyhow::ensure!(
            response.status() == reqwest::StatusCode::BAD_REQUEST,
            "status {}",
            response.status()
        );
        let body: serde_json::Value = response.json().await?;
        anyhow::ensure!(
            body["error"]["code"] == "invalid_request_error",
            "body: {body}"
        );
        Ok(())
    })
}

/// A client disconnecting mid-request over HTTP/2 cancels the underlying
/// Generation, the same as `/v1/chat/completions` (ADR 0003, ADR 0006).
#[test]
fn responses_client_disconnect_cancels_generation() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        h.fake.set_mode(
            FakeMode::reply("too slow for responses").with_delay(Duration::from_secs(20)),
        );
        let marker = h.db_now().await?;

        let request = h
            .http2
            .post(h.url("/v1/responses"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&json!({"model": h.model_alias, "input": "drop me", "stream": false}))
            .send();
        let _ = tokio::time::timeout(Duration::from_millis(800), request).await;

        let row = wait_until(
            || h.generation_row_created_after(h.tenant1.id, marker),
            Duration::from_secs(5),
        )
        .await?;
        let cancelled = wait_until(
            || async {
                let row = h
                    .generation_row(h.tenant1.id, row.id)
                    .await?
                    .context("generation disappeared")?;
                Ok((row.state == "cancelled").then_some(row))
            },
            Duration::from_secs(30),
        )
        .await?;
        anyhow::ensure!(cancelled.state == "cancelled");

        h.fake.set_mode(FakeMode::reply("responses recovered"));
        Ok(())
    })
}

/// Tears the shared `Harness` down: kills the `gpq-remote`/`gpq-worker`
/// child processes, drops the per-run database and login role, and removes
/// the run's temp directory. Named to sort after every `*_...` test above so
/// it runs last under `--test-threads=1`, whether or not earlier tests
/// failed.
#[test]
fn zzz_teardown_harness() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(Harness::teardown(h))
}
