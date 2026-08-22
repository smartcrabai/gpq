# Start with one Remote instance

The initial deployment runs one Remote instance and uses PostgreSQL as its only durable state; Worker reconnection and leased Attempts recover from process restarts. Remote remains otherwise stateless so replicas can be added later, avoiding distributed ownership of long-lived Worker streams before production demand justifies it.
