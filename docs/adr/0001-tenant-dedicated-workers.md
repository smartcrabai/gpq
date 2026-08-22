# Keep workers tenant-dedicated

Every Worker is permanently scoped to one Tenant and executes only that Tenant's Generations. This sacrifices cross-tenant GPU pooling, but keeps credentials, plaintext generation data, model access, scheduling, and failure impact inside one explicit trust boundary.
