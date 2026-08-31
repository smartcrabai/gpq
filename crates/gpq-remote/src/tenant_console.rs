//! Tenant-scoped browser console served by `gpq-remote`.
//!
//! The page is a static same-origin client of the existing `TenantService`.
//! It never adds an administration API or gives `serve` broader database
//! privileges: lifecycle and credential operations remain local CLI commands
//! (ADR 0009, ADR 0016).

use axum::http::header::{self, HeaderName, HeaderValue};
use axum::response::Html;
use axum::routing::get;

const INDEX: &str = include_str!("tenant_console.html");
const CONTENT_SECURITY_POLICY: HeaderName = HeaderName::from_static("content-security-policy");
const PERMISSIONS_POLICY: HeaderName = HeaderName::from_static("permissions-policy");

/// Routes for the Tenant console. No application state is needed because the
/// page calls the existing same-origin `TenantService` endpoints directly.
#[must_use = "the router does nothing until merged into the served application"]
pub fn router() -> axum::Router {
    axum::Router::new()
        .route("/console", get(index))
        .route("/console/", get(index))
}

async fn index() -> ([(HeaderName, HeaderValue); 5], Html<&'static str>) {
    (
        [
            (header::CACHE_CONTROL, HeaderValue::from_static("no-store")),
            (
                CONTENT_SECURITY_POLICY,
                HeaderValue::from_static(
                    "default-src 'none'; connect-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'",
                ),
            ),
            (
                header::REFERRER_POLICY,
                HeaderValue::from_static("no-referrer"),
            ),
            (
                header::X_CONTENT_TYPE_OPTIONS,
                HeaderValue::from_static("nosniff"),
            ),
            (
                PERMISSIONS_POLICY,
                HeaderValue::from_static("camera=(), microphone=(), geolocation=()"),
            ),
        ],
        Html(INDEX),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn console_route_serves_the_browser_client_with_security_headers() -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move { axum::serve(listener, router()).await });

        let response = reqwest::get(format!("http://{address}/console")).await?;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("text/html; charset=utf-8")
        );
        assert_eq!(
            response
                .headers()
                .get(reqwest::header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some("no-store")
        );
        assert!(response.headers().contains_key("content-security-policy"));

        let body = response.text().await?;
        assert!(body.contains("<title>GPQ Tenant Console</title>"));
        assert!(body.contains("/gpq.v1.TenantService/${method}"));

        server.abort();
        Ok(())
    }
}
