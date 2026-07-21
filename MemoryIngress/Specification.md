# Kennedy Memory Ingress

`kennedy-memory-ingress` owns the one durable queue that serializes memory
updates from every prepared source. ConversationHistory submits closed Chatend
archives and AudioIngress submits ordered transcript pieces. Neither source
owns a claim or retry queue.

Each job records a stable `(source_kind, source_id)` identity, the source's
historical timestamp and within-source position, its model-loop checkpoint,
provenance ID, optimistic version, retry schedule, and bounded failure history.
Across both source kinds, at most one job may be `ingress_in_progress`.
Selection resumes that job first and otherwise chooses the oldest eligible
source and position.

Submission and transition methods are idempotent where a lost response may be
replayed. Completion requires a successful `EndTurn` receipt in the durable
history-ingress checkpoint. An input-too-large error or fifth consecutive
failure becomes terminal; an explicit retry preserves diagnostics while
resetting the consecutive count.

On first adoption, source libraries import pending, claimed, and failed jobs
from their legacy tables. A claimed job is released with a short delay because
its worker cannot survive the process restart. Source records continue to
mirror queue fields for browser compatibility, but the shared database is the
authority for selection and transitions.
