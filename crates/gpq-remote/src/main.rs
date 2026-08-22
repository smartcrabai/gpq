//! Binary shim: every subcommand lives in [`gpq_remote::cli`] so the same
//! code is reachable from the integration suites (see `lib.rs`).

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    gpq_remote::cli::run().await
}
