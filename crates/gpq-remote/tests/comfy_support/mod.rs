//! A fake `ComfyUI` implementing exactly the surface
//! `crates/gpq-worker/src/backend/comfy.rs` speaks: `GET /system_stats`, `GET
//! /object_info`, `GET /history` (bare, and by `prompt_id`), `GET /view`,
//! `POST /prompt`, `POST /interrupt`, `POST /free`, and `GET
//! /ws?clientId=`. Runs inside the test process on an ephemeral loopback
//! port.
//!
//! `POST /prompt` inspects the submitted graph to pick one of three
//! scenarios, keyed by well-known node `class_type`s a real `ComfyUI`
//! backend would never emit on its own, so a single fake server can drive
//! every test in this suite without per-test wiring:
//!
//! - A `CheckpointLoaderSimple` node naming an unknown checkpoint fails
//!   `/prompt` itself with a `"not in list"` validation error, the same
//!   shape `classify_prompt_validation_error` classifies as
//!   `ModelUnavailable` (ADR 0012).
//! - A node whose `class_type` is [`OOM_NODE_CLASS`] accepts `/prompt` and
//!   then reports an `execution_error` with an out-of-memory exception
//!   (ADR 0003).
//! - A node whose `class_type` is [`HANG_NODE_CLASS`] accepts `/prompt`,
//!   starts executing, and then never finishes on its own: it waits for
//!   `POST /interrupt` before reporting `execution_interrupted` (ADR 0003).
//! - Anything else runs to completion: `execution_start`, `executing`, a
//!   handful of `progress` frames, `executed` naming a real served image,
//!   and `execution_success`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::{Notify, mpsc};
use uuid::Uuid;

/// A minimal but genuine PNG byte payload `GET /view` serves, so the Worker
/// hashes and publishes a real output Artifact from real transferred bytes
/// (ADR 0008).
pub const IMAGE_BYTES: &[u8] = &[
    0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, b'I', b'H', b'D', b'R',
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x77, 0x53,
    0xDE, 0x00, 0x00, 0x00, 0x0C, b'I', b'D', b'A', b'T', 0x78, 0x9C, 0x63, 0xF8, 0xCF, 0xC0, 0x00,
    0x00, 0x03, 0x01, 0x01, 0x00, 0x18, 0xDD, 0x8D, 0xB0, 0x00, 0x00, 0x00, 0x00, b'I', b'E', b'N',
    b'D', 0xAE, 0x42, 0x60, 0x82,
];

/// The checkpoint filename a `CheckpointLoaderSimple` node must name to pass
/// `/prompt` validation; anything else fails the way real `ComfyUI` rejects
/// an unresolved checkpoint (ADR 0012).
pub const KNOWN_CHECKPOINT: &str = "fake-model.safetensors";

/// The `output_node`/`output_name` every `WorkflowManifest` registered
/// against this fake should declare (ADR 0007).
pub const OUTPUT_NODE: &str = "9";
pub const OUTPUT_NAME: &str = "images";

/// A graph node `class_type` this fake treats as "accept `/prompt`, then
/// fail execution with an out-of-memory `execution_error`" (ADR 0003).
pub const OOM_NODE_CLASS: &str = "OOMTrigger";
/// A graph node `class_type` this fake treats as "accept `/prompt`, start
/// executing, then wait indefinitely for `POST /interrupt`" (ADR 0003).
pub const HANG_NODE_CLASS: &str = "HangUntilInterrupted";

const OUTPUT_FILENAME: &str = "fake-output.png";
const PROGRESS_STEPS: u64 = 4;
const STEP_DELAY: Duration = Duration::from_millis(30);

/// The SHA-256 the Worker must compute for [`IMAGE_BYTES`], as lowercase
/// hex, matching `gpq_domain::hash::Hasher`'s output shape.
#[must_use]
pub fn image_digest_hex() -> String {
    let mut hasher = Sha256::new();
    hasher.update(IMAGE_BYTES);
    hex::encode(hasher.finalize())
}

fn base_graph() -> Map<String, Value> {
    let mut graph = Map::new();
    graph.insert(
        "1".to_owned(),
        json!({"class_type": "CheckpointLoaderSimple", "inputs": {"ckpt_name": KNOWN_CHECKPOINT}}),
    );
    graph.insert(
        OUTPUT_NODE.to_owned(),
        json!({"class_type": "SaveImage", "inputs": {"images": ["1", 0]}}),
    );
    graph
}

/// A graph the fake accepts and completes successfully after
/// [`PROGRESS_STEPS`] `progress` frames.
#[must_use]
pub fn success_graph() -> Value {
    Value::Object(base_graph())
}

/// [`success_graph`] plus a node whose `class_type` is [`OOM_NODE_CLASS`].
#[must_use]
pub fn oom_graph() -> Value {
    let mut graph = base_graph();
    graph.insert(
        "2".to_owned(),
        json!({"class_type": OOM_NODE_CLASS, "inputs": {}}),
    );
    Value::Object(graph)
}

/// [`success_graph`] plus a node whose `class_type` is [`HANG_NODE_CLASS`].
#[must_use]
pub fn hang_graph() -> Value {
    let mut graph = base_graph();
    graph.insert(
        "2".to_owned(),
        json!({"class_type": HANG_NODE_CLASS, "inputs": {}}),
    );
    Value::Object(graph)
}

/// A graph whose `CheckpointLoaderSimple` names a checkpoint the fake does
/// not recognize, so `/prompt` rejects it outright (ADR 0012).
#[must_use]
pub fn model_unavailable_graph() -> Value {
    let mut graph = Map::new();
    graph.insert(
        "1".to_owned(),
        json!({"class_type": "CheckpointLoaderSimple", "inputs": {"ckpt_name": "missing-model.safetensors"}}),
    );
    graph.insert(
        OUTPUT_NODE.to_owned(),
        json!({"class_type": "SaveImage", "inputs": {"images": ["1", 0]}}),
    );
    Value::Object(graph)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scenario {
    Success,
    Oom,
    Hang,
}

struct PromptRecord {
    client_id: String,
    scenario: Scenario,
    interrupt: Arc<Notify>,
}

#[derive(Default)]
struct Inner {
    ws_clients: HashMap<String, mpsc::UnboundedSender<Message>>,
    prompts: HashMap<String, PromptRecord>,
    history: HashMap<String, Value>,
    interrupted: HashSet<String>,
    last_prompt_graph: Option<Value>,
}

fn lock(mutex: &Mutex<Inner>) -> MutexGuard<'_, Inner> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Handle to the running fake `ComfyUI`.
#[derive(Clone)]
pub struct FakeComfy {
    inner: Arc<Mutex<Inner>>,
    base_url: String,
}

impl FakeComfy {
    /// Binds an ephemeral loopback port and serves the fake for the rest of
    /// the test process.
    ///
    /// # Errors
    /// Returns an error if the loopback listener cannot be bound.
    pub async fn spawn() -> anyhow::Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let handle = Self {
            inner: Arc::new(Mutex::new(Inner::default())),
            base_url: format!("http://127.0.0.1:{port}"),
        };
        let router = Router::new()
            .route("/system_stats", get(system_stats))
            .route("/object_info", get(object_info))
            .route("/history", get(history_probe))
            .route("/history/{prompt_id}", get(history_by_id))
            .route("/view", get(view))
            .route("/prompt", post(prompt))
            .route("/interrupt", post(interrupt))
            .route("/free", post(free))
            .route("/ws", get(ws_upgrade))
            .with_state(handle.clone());
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        Ok(handle)
    }

    /// The loopback base URL a Worker `Pool` should point at.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// How many distinct prompts have received `POST /interrupt` so far.
    /// Monotonically increasing, so a caller records the count before
    /// cancelling and waits for it to increase (ADR 0003).
    #[must_use]
    pub fn interrupted_count(&self) -> usize {
        lock(&self.inner).interrupted.len()
    }

    /// Most recent graph accepted by `POST /prompt`.
    #[must_use]
    pub fn last_prompt_graph(&self) -> Option<Value> {
        lock(&self.inner).last_prompt_graph.clone()
    }

    fn send_to_client(&self, client_id: &str, message: Message) {
        let sender = lock(&self.inner).ws_clients.get(client_id).cloned();
        if let Some(sender) = sender {
            let _ = sender.send(message);
        }
    }

    fn register_client(&self, client_id: String, sender: mpsc::UnboundedSender<Message>) {
        lock(&self.inner).ws_clients.insert(client_id, sender);
    }

    fn unregister_client(&self, client_id: &str) {
        lock(&self.inner).ws_clients.remove(client_id);
    }

    fn register_prompt(&self, prompt_id: String, client_id: String, scenario: Scenario) {
        let notify = Arc::new(Notify::new());
        lock(&self.inner).prompts.insert(
            prompt_id,
            PromptRecord {
                client_id,
                scenario,
                interrupt: notify,
            },
        );
    }

    fn record_prompt_graph(&self, graph: Value) {
        lock(&self.inner).last_prompt_graph = Some(graph);
    }

    fn record_history(&self, prompt_id: &str, node: &str, output: Value) {
        let mut inner = lock(&self.inner);
        let entry = inner
            .history
            .entry(prompt_id.to_owned())
            .or_insert_with(|| json!({"outputs": {}}));
        entry["outputs"][node] = output;
    }

    fn history_snapshot(&self, prompt_id: &str) -> Option<Value> {
        lock(&self.inner).history.get(prompt_id).cloned()
    }

    fn note_interrupt(&self, prompt_id: &str) -> Option<Arc<Notify>> {
        let mut inner = lock(&self.inner);
        inner.interrupted.insert(prompt_id.to_owned());
        inner
            .prompts
            .get(prompt_id)
            .map(|record| record.interrupt.clone())
    }

    fn prompt_context(&self, prompt_id: &str) -> Option<(String, Scenario, Arc<Notify>)> {
        lock(&self.inner).prompts.get(prompt_id).map(|record| {
            (
                record.client_id.clone(),
                record.scenario,
                record.interrupt.clone(),
            )
        })
    }

    fn spawn_driver(&self, prompt_id: String) {
        let Some((client_id, scenario, interrupt)) = self.prompt_context(&prompt_id) else {
            return;
        };
        let state = self.clone();
        tokio::spawn(async move {
            state.send_to_client(
                &client_id,
                event("execution_start", &json!({"prompt_id": prompt_id})),
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
            state.send_to_client(
                &client_id,
                event("executing", &json!({"prompt_id": prompt_id, "node": "1"})),
            );
            match scenario {
                Scenario::Success => {
                    for step in 1..=PROGRESS_STEPS {
                        tokio::time::sleep(STEP_DELAY).await;
                        state.send_to_client(
                            &client_id,
                            event(
                                "progress",
                                &json!({"prompt_id": prompt_id, "node": "1", "value": step, "max": PROGRESS_STEPS}),
                            ),
                        );
                    }
                    let output = json!({OUTPUT_NAME: [{"filename": OUTPUT_FILENAME, "subfolder": "", "type": "output"}]});
                    state.record_history(&prompt_id, OUTPUT_NODE, output.clone());
                    state.send_to_client(
                        &client_id,
                        event(
                            "executed",
                            &json!({"prompt_id": prompt_id, "node": OUTPUT_NODE, "output": output}),
                        ),
                    );
                    state.send_to_client(
                        &client_id,
                        event("execution_success", &json!({"prompt_id": prompt_id})),
                    );
                }
                Scenario::Oom => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    state.send_to_client(
                        &client_id,
                        event(
                            "execution_error",
                            &json!({
                                "prompt_id": prompt_id,
                                "exception_type": "OutOfMemoryError",
                                "exception_message": "CUDA out of memory. Tried to allocate 20.00 GiB",
                            }),
                        ),
                    );
                }
                Scenario::Hang => {
                    interrupt.notified().await;
                    state.send_to_client(
                        &client_id,
                        event("execution_interrupted", &json!({"prompt_id": prompt_id})),
                    );
                }
            }
        });
    }
}

fn event(kind: &str, data: &Value) -> Message {
    Message::Text(json!({"type": kind, "data": data}).to_string().into())
}

/// Mirrors upstream `ComfyUI`'s `/system_stats`, whose `comfy_package_versions`
/// is an array of `{name, installed, required}` entries.
async fn system_stats() -> Json<Value> {
    Json(json!({
        "system": {
            "comfyui_version": "1.0.0-fake",
            "comfy_package_versions": [
                {"name": "comfyui-frontend-package", "installed": "1.0.0", "required": "1.0.0"}
            ],
        },
        "devices": [{"vram_free": 8_000_000_000_u64, "vram_total": 8_000_000_000_u64}],
    }))
}

/// Every node class this fake understands, all reported with the core
/// `"nodes"` `python_module` so none of them is ever derived as an
/// installed custom-node package (ADR 0007, ADR 0018): every
/// `required_custom_nodes` entry this suite exercises is genuinely absent.
async fn object_info() -> Json<Value> {
    let classes = [
        "CheckpointLoaderSimple",
        "SaveImage",
        OOM_NODE_CLASS,
        HANG_NODE_CLASS,
    ];
    let mut map = Map::new();
    for class_type in classes {
        map.insert(class_type.to_owned(), json!({"python_module": "nodes"}));
    }
    Json(Value::Object(map))
}

async fn history_probe() -> Json<Value> {
    Json(json!({}))
}

async fn history_by_id(
    State(state): State<FakeComfy>,
    Path(prompt_id): Path<String>,
) -> Json<Value> {
    let entry = state
        .history_snapshot(&prompt_id)
        .unwrap_or_else(|| json!({"outputs": {}}));
    let mut body = Map::new();
    body.insert(prompt_id, entry);
    Json(Value::Object(body))
}

async fn view() -> Response {
    ([(header::CONTENT_TYPE, "image/png")], IMAGE_BYTES).into_response()
}

fn checkpoint_name(graph: &Map<String, Value>) -> Option<String> {
    for node in graph.values() {
        let class_type = node.get("class_type").and_then(Value::as_str);
        if class_type == Some("CheckpointLoaderSimple") {
            return node
                .get("inputs")
                .and_then(|inputs| inputs.get("ckpt_name"))
                .and_then(Value::as_str)
                .map(str::to_owned);
        }
    }
    None
}

fn has_node_class(graph: &Map<String, Value>, class_type: &str) -> bool {
    graph
        .values()
        .any(|node| node.get("class_type").and_then(Value::as_str) == Some(class_type))
}

fn no_prompt_response() -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({
            "error": {"type": "no_prompt", "message": "No prompt provided", "details": "", "extra_info": {}},
            "node_errors": {},
        })),
    )
        .into_response()
}

fn model_not_in_list_response(ckpt: &str) -> Response {
    let details = format!("Value not in list: ckpt_name: '{ckpt}' not in ['{KNOWN_CHECKPOINT}']");
    (
        StatusCode::BAD_REQUEST,
        Json(json!({
            "error": {
                "type": "prompt_outputs_failed_validation",
                "message": "Prompt outputs failed validation",
                "details": details,
                "extra_info": {},
            },
            "node_errors": {},
        })),
    )
        .into_response()
}

async fn prompt(State(state): State<FakeComfy>, Json(body): Json<Value>) -> Response {
    let Some(obj) = body.as_object() else {
        return no_prompt_response();
    };
    let Some(graph) = obj
        .get("prompt")
        .and_then(Value::as_object)
        .filter(|graph| !graph.is_empty())
    else {
        return no_prompt_response();
    };
    if let Some(ckpt) = checkpoint_name(graph)
        && ckpt != KNOWN_CHECKPOINT
    {
        return model_not_in_list_response(&ckpt);
    }
    state.record_prompt_graph(Value::Object(graph.clone()));
    let client_id = obj
        .get("client_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let scenario = if has_node_class(graph, OOM_NODE_CLASS) {
        Scenario::Oom
    } else if has_node_class(graph, HANG_NODE_CLASS) {
        Scenario::Hang
    } else {
        Scenario::Success
    };
    let prompt_id = Uuid::now_v7().to_string();
    state.register_prompt(prompt_id.clone(), client_id, scenario);
    state.spawn_driver(prompt_id.clone());
    Json(json!({"prompt_id": prompt_id, "number": 0, "node_errors": {}})).into_response()
}

async fn interrupt(State(state): State<FakeComfy>, Json(body): Json<Value>) -> Response {
    if let Some(prompt_id) = body.get("prompt_id").and_then(Value::as_str)
        && let Some(notify) = state.note_interrupt(prompt_id)
    {
        notify.notify_one();
    }
    StatusCode::OK.into_response()
}

async fn free() -> Json<Value> {
    Json(json!({}))
}

async fn ws_upgrade(
    ws: WebSocketUpgrade,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<FakeComfy>,
) -> Response {
    let client_id = params.get("clientId").cloned().unwrap_or_default();
    ws.on_upgrade(move |socket| handle_socket(socket, state, client_id))
}

async fn handle_socket(mut socket: WebSocket, state: FakeComfy, client_id: String) {
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    state.register_client(client_id.clone(), tx);
    if socket
        .send(event(
            "status",
            &json!({"status": {"exec_info": {"queue_remaining": 0}}}),
        ))
        .await
        .is_err()
    {
        state.unregister_client(&client_id);
        return;
    }
    loop {
        tokio::select! {
            outgoing = rx.recv() => {
                let Some(message) = outgoing else { break; };
                if socket.send(message).await.is_err() {
                    break;
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(_)) => {}
                    _ => break,
                }
            }
        }
    }
    state.unregister_client(&client_id);
}
