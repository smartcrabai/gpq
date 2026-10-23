# Unify the Image and Video Generation lifecycle

Image and Video Generations share a lifecycle envelope, durable storage policy, priority, seed, progress events, and Artifact handling, but use typed `GenerationParameters` rather than opaque backend payloads. `ImageParameters` and `VideoParameters` define the complete request shapes; each Model Family determines its modality and supplies its pinned sampling preset. The native contract contains no LLM/chat, music, workflow graph, or raw prompt Generation types.
