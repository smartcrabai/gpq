# Do not sandbox ComfyUI custom nodes

Worker runs only operator-installed custom nodes under its own OS account and never installs code requested by Remote; workflows referring to absent nodes are rejected. Cross-platform process sandboxing is excluded because the Worker is already a tenant-owned trust boundary, so operators needing stronger filesystem or network isolation must place the entire Worker in a VM or container.
