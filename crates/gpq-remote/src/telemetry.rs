//! OTLP tracing setup shared by every `gpq-remote` subcommand.
//!
//! Both binaries in this workspace export traces over OTLP/gRPC so operators
//! can correlate Generation lifecycles across Remote and Worker processes; the
//! wire protocols and multi-tenant isolation rules that make that correlation
//! meaningful come from ADR 0001/0004/0011, but tracing setup itself is
//! cross-cutting infrastructure rather than a modeled ADR decision.

use anyhow::Context;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

/// Keeps the OTLP tracer provider alive for the process lifetime and flushes
/// buffered spans on drop so a graceful shutdown does not lose them.
pub struct TelemetryGuard {
    provider: SdkTracerProvider,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Err(error) = self.provider.shutdown() {
            eprintln!("failed to shut down OTLP tracer provider: {error:?}");
        }
    }
}

/// Installs the global `tracing` subscriber with an OTLP/gRPC span exporter
/// and a `tracing-subscriber` `fmt` layer, filtered by `RUST_LOG` (default
/// `info`).
///
/// # Errors
/// Returns an error if the OTLP exporter cannot be built (e.g. an invalid
/// endpoint) or if a global subscriber is already installed.
pub fn init(service: &str) -> anyhow::Result<TelemetryGuard> {
    let exporter = SpanExporter::builder()
        .with_tonic()
        .build()
        .context("failed to build the OTLP span exporter")?;

    let resource = Resource::builder()
        .with_service_name(service.to_string())
        .build();

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer(service.to_string());
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer();

    Registry::default()
        .with(env_filter)
        .with(fmt_layer)
        .with(otel_layer)
        .try_init()
        .context("failed to install the global tracing subscriber")?;

    Ok(TelemetryGuard { provider })
}
