# Terminate TLS at ingress

Production clients always use TLS, but certificate management and ACME remain outside Remote: an HTTP/2-capable reverse proxy or ingress terminates TLS and forwards private HTTP/1.1 and h2c, with long-lived Worker gRPC streams exempt from proxy timeouts. Remote is never exposed directly to the Internet, development may use loopback plaintext, and bearer credentials replace mTLS.
