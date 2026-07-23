# Kennedy Memory Ingress

`kennedy-memory-ingress` is the durable work queue used by prepared audio
ingress. AudioIngress submits ordered transcript pieces; the orchestration
worker claims and checkpoints them through one globally serialized Kweb write
lane.

Conversational history ingress no longer uses this queue. A source session and
its history-ingress continuation now remain in the same append-only Chatend
journal and produce one final Kweb transaction. The `Conversation` source-kind
value remains decodable only so the existing queue database format is stable;
the cutover archived and removed all live conversation rows, and the runtime
rejects any such row as a cutover invariant violation.

Each audio job records a stable source identity, historical timestamp,
within-source position, model-loop checkpoint, optimistic version, retry
schedule, and bounded failure history. At most one job may be
`ingress_in_progress`. Selection resumes that job first and otherwise chooses
the oldest eligible audio position.

Submission and transition methods are idempotent where a lost response may be
replayed. Completion requires a successful final Kweb session commit. An
input-too-large error or fifth consecutive failure becomes terminal; an
explicit retry preserves diagnostics while resetting the consecutive count.

The queue remains SQLite because it is specialized intake state, not Kweb
content or Session History. It contains no completed Chatend archives and does
not translate or load legacy conversation sessions.
