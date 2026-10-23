//! Consumer behavior for generation-scoped failed-pool dispatch history.

use super::support::{
    AttemptSpec, GenerationSpec, Harness, content_hash, insert_attempt, insert_device_pool,
    insert_generation, insert_tenant, insert_worker, tenant_tx,
};
use chrono::Utc;
use gpq_domain::{DevicePoolId, GenerationId, TenantId};
use gpq_remote::db::native_exclusions;
use uuid::Uuid;

struct PoolExclusionFixture {
    tenant_uuid: Uuid,
    tenant: TenantId,
    oom_pool: DevicePoolId,
    rejected_pool: DevicePoolId,
    generation: GenerationId,
    other_generation: GenerationId,
    generation_uuid: Uuid,
}

async fn pool_exclusion_fixture(harness: &Harness) -> anyhow::Result<PoolExclusionFixture> {
    let pool = harness.pool();
    let tenant_uuid = Uuid::now_v7();
    let tenant = TenantId::from_uuid(tenant_uuid);
    let worker_uuid = Uuid::now_v7();
    let oom_pool_uuid = Uuid::now_v7();
    let rejected_pool_uuid = Uuid::now_v7();
    let oom_pool = DevicePoolId::from_uuid(oom_pool_uuid);
    let rejected_pool = DevicePoolId::from_uuid(rejected_pool_uuid);
    insert_tenant(pool, tenant_uuid, "native-dispatch").await?;
    insert_worker(
        pool,
        tenant_uuid,
        worker_uuid,
        "dispatch-worker",
        &[1, 2, 3],
        None,
    )
    .await?;
    insert_device_pool(pool, tenant_uuid, oom_pool_uuid, worker_uuid, "gpu-a", 1, 1).await?;
    insert_device_pool(
        pool,
        tenant_uuid,
        rejected_pool_uuid,
        worker_uuid,
        "gpu-b",
        1,
        1,
    )
    .await?;

    let mut first = GenerationSpec::new(tenant_uuid, content_hash("native-dispatch-model"));
    first.attempt_count = 1;
    let generation_uuid = insert_generation(pool, &first).await?;
    let generation = GenerationId::from_uuid(generation_uuid);
    let second = GenerationSpec::new(tenant_uuid, content_hash("native-dispatch-model"));
    let other_generation = GenerationId::from_uuid(insert_generation(pool, &second).await?);

    let mut oom_attempt = AttemptSpec::new(
        tenant_uuid,
        generation_uuid,
        worker_uuid,
        oom_pool_uuid,
        Utc::now(),
    );
    oom_attempt.state = "failed";
    let oom_attempt_id = insert_attempt(pool, &oom_attempt).await?;
    sqlx::query(
        "UPDATE attempts SET failure_kind = 'out_of_memory' \
         WHERE tenant_id = $1 AND id = $2",
    )
    .bind(tenant_uuid)
    .bind(oom_attempt_id)
    .execute(pool)
    .await?;

    Ok(PoolExclusionFixture {
        tenant_uuid,
        tenant,
        oom_pool,
        rejected_pool,
        generation,
        other_generation,
        generation_uuid,
    })
}

#[tokio::test]
async fn pool_exclusions_combine_oom_and_preexecution_rejections_per_generation()
-> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let fixture = pool_exclusion_fixture(&harness).await?;
    let pool = harness.pool();
    let mut tx = tenant_tx(pool, fixture.tenant_uuid).await?;
    assert!(
        native_exclusions::record_preexecution_rejection(
            &mut tx,
            fixture.tenant,
            fixture.generation,
            fixture.rejected_pool,
        )
        .await?
    );
    for _ in 0..5 {
        assert!(
            !native_exclusions::record_preexecution_rejection(
                &mut tx,
                fixture.tenant,
                fixture.generation,
                fixture.rejected_pool,
            )
            .await?
        );
    }

    let excluded =
        native_exclusions::excluded_pools(&mut tx, fixture.tenant, fixture.generation).await?;
    assert_eq!(excluded.len(), 2);
    assert!(excluded.contains(&fixture.oom_pool));
    assert!(excluded.contains(&fixture.rejected_pool));
    assert!(
        native_exclusions::excluded_pools(&mut tx, fixture.tenant, fixture.other_generation)
            .await?
            .is_empty()
    );
    tx.commit().await?;

    let attempt_count: i32 = sqlx::query_scalar(
        "SELECT attempt_count FROM generations WHERE tenant_id = $1 AND id = $2",
    )
    .bind(fixture.tenant_uuid)
    .bind(fixture.generation_uuid)
    .fetch_one(pool)
    .await?;
    let rejection_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM generation_pool_rejections \
         WHERE tenant_id = $1 AND generation_id = $2",
    )
    .bind(fixture.tenant_uuid)
    .bind(fixture.generation_uuid)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        attempt_count, 1,
        "pre-execution rejection is not a runtime attempt"
    );
    assert_eq!(
        rejection_rows, 1,
        "repeated rejection of one pool stays bounded"
    );

    harness.teardown().await
}
