# Shared Codex Runtime

`kennedy-codex-runtime` is a library, not an HTTP service. It owns the model
catalog work shared by Intelligence and AudioIngress so both services use the
same sanitized catalog and never launch duplicate discovery processes.

## Catalog cache

Every Kennedy process runs `codex-safe --version`. The runtime hashes that
identity together with the executable name and its own cache-schema version.
The cache lives at `CODEX_SAFE_CATALOG_DIR` when configured, otherwise at
`${TMPDIR:-/tmp}/kennedy-codex-catalogs`. The same absolute directory must be
mounted read-only inside the Codex container.

On a valid cache hit, the runtime parses the advertised limits and rechecks
that hidden instructions remain absent; it does not rediscover, rewrite, or
re-probe the catalog. On a miss, it runs `codex-safe debug models` once,
removes provider base instructions, model messages, model-selected skills, and
agent-tool selectors, verifies that effective context limits are unchanged,
and atomically writes the result. A second `debug models` call confirms that
Codex can consume the sanitized file before it becomes a cache hit.

Clones of `CatalogCache` share one asynchronous initialization cell. Concurrent
Intelligence and AudioIngress startup therefore performs exactly one catalog
load or discovery.

## Prompt-boundary validation

`codex-safe debug prompt-input` reports Codex's formatted input list before the
Responses Lite request builder adds its native-tool and base-instruction
developer items. Kennedy supplies a sentinel and requires the reported list to
contain exactly that one user message. This detects accidental project rules,
skills, or other extra prompt items at that boundary. Launcher tests separately
pin every approved instruction setting, including Intelligence's one fixed
Kennedy tool-harness base instruction and AudioIngress's empty instructions.

Intelligence and AudioIngress use different Codex configurations, so each owns
one boundary-validation scope. A successful result is cached under the Codex
catalog identity and that explicit configuration scope. It runs again when the
Codex version, catalog sanitization schema, model, reasoning configuration, or
scope version changes. A failed validation is never cached.

ChatGPT login status is deliberately not cached because authentication can
change independently of the Codex version or model catalog.
