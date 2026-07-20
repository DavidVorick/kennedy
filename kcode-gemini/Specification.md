# kcode-gemini Specification

## 1. Purpose and boundary

`kcode-gemini` is a standalone Rust 2024 library imported by a larger server.
It owns Gemini HTTP transport, fixed supported-model selection, validation,
Interactions API serialization and normalization, credential lifetime,
in-memory session usage accounting, published-price cost calculation, and
live-session spending limits.

It is not an HTTP server and defines no wire contract. The integrating server
owns secret retrieval, authentication, authorization, routes, serialization,
listener policy, cancellation, logging, and mapping Rust errors to its own API.

## 2. Supported provider surface

The compiled model identifiers are:

- `gemini-3.1-flash-lite` for text inference and grounded search;
- `gemini-3.1-pro-preview` for text and image/audio multimodal inference; and
- `gemini-3-pro-image` for Nano Banana Pro generation/editing.

Gemini 3.5 Flash and Nano Banana 2 are intentionally absent. Arbitrary model
strings are not accepted, preventing callers from bypassing the supported and
priced model set.

Calls use `POST /v1beta/interactions`, `store: false`, explicit generation
configuration, and the API key in `x-goog-api-key`. Text output is concatenated
from `model_output` text blocks. Native image blocks are base64-decoded and
returned as bytes. `completed` and usable `incomplete` responses succeed;
other states and missing required output are protocol errors.

`grounded_search` uses the built-in `google_search` tool and normalizes URL
citations before fallback search results. Only HTTP(S) URLs are retained;
fragments are removed, duplicates are suppressed, titles are control-cleaned,
and the caller's source cap is enforced.

## 3. Inputs and outputs

Prompts and optional system instructions are non-empty and individually capped
at 4 MiB. Text calls accept no media. Pro multimodal calls require at least one
image or audio input. Nano Banana Pro permits zero or more images and rejects
audio. Media inputs must be non-empty, aggregate raw media is capped at 64 MiB,
and Nano Banana Pro accepts at most 14 reference images.

Media is base64-encoded inline and is not retained through the Google Files
API. The default output cap is 8,192 tokens. Text calls allow 1 through 65,536;
image calls allow 1 through 32,768. Temperature, when supplied, is finite and
between 0 and 2 inclusive. Pro rejects `minimal` thinking. Nano Banana Pro uses
Standard service only.

Nano Banana Pro output resolution is fixed by the library at 2K and is not a
request field. Supported caller-selectable aspect ratios are 1:1, 2:3, 3:2,
3:4, 4:3, 4:5, 5:4, 9:16, 16:9, and 21:9.

Provider response bodies are limited to 128 MiB. Provider error bodies are
reduced to a control-cleaned code and 400-character message and are never
recorded verbatim. Transport errors are reduced to timeout or reachability
classes rather than exposing request internals.

## 4. Credentials and network safety

Construction takes ownership of one API key, trims it, rejects empty or
invalid-header values, and starts a new accounting session. The temporary
supplied string and retained string use zeroizing storage. The key is shared
across clones, redacted from `Debug`, and never serialized or recorded.

The HTTPS-only HTTP client is built without environment proxy discovery,
redirects, referrers, or automatic retries and places the key only in a
sensitive `x-goog-api-key` header. It never puts the key in a URL or JSON. The
credential-bearing `reqwest` and `zeroize` dependencies are exact-version
pinned and reviewed in `DependencyAudit.md`.

Zeroization is best-effort memory hygiene, not encryption: it overwrites the
owned key buffer when its final owner is dropped, but cannot retroactively
erase old reallocation buffers, compiler-created copies, or copies made inside
the networking and TLS stacks.

## 5. In-memory session accounting

`Gemini::open(key)` creates a new in-memory state containing a monotonic record
identifier, usage records, and three optional limits. Clones share the same
state. The crate opens no database, creates no files, serializes no accounting
state, and offers no save/load operation. Dropping the last clone destroys all
records and limits. Opening another client always starts empty.

Each record contains only completion time, stable operation and model names,
service tier, success state, content-free failure class, optional provider
interaction ID, provider token/grounding usage, and locally calculated cost.
It does not contain the key, request bodies, prompts, system instructions,
media, response text, generated images, citations, raw errors, or headers.

Reports select current-session all-time, current UTC hour/day/month, or an
explicit half-open UTC range. They return totals and deterministic groups by
model and operation. Individual records are newest-first and capped at 10,000
per call. Money is an integer count of one-billionth USD; accumulation
saturates rather than wrapping. Public report types are ordinary Rust values;
no route-specific Serde or wire representation is part of the contract.

## 6. Pricing and billing authority

The pricing table is identified as `google-gemini-2026-07-20` and includes the
published Standard and Priority token rates for Flash-Lite and Pro, plus
Standard Nano Banana Pro rates. Pro switches rates when total input exceeds
200,000 tokens. Flash-Lite applies the distinct audio input/cache rate.
Documents use non-audio input rates. Nano Banana Pro uses $2 per million input
tokens, $12 per million text/thinking output tokens, and $120 per million image
output tokens; a 1K or 2K generated image is reported as 1,120 image tokens.

Provider modality fields drive calculation. Missing optional detail uses a
documented fallback and marks the result `Estimated`. Search query cost assumes
$0.014 for every provider-reported query and marks the result `Conservative`.

The local calculation cannot observe external usage, credits, taxes, contract
pricing, delayed billing adjustments, or future price changes. It is therefore
a session-scoped estimate, not authoritative spend. Google AI Studio and Cloud
Billing are authoritative. Actual costs can be queried programmatically only
through separately configured Google Cloud Billing export data (for example,
BigQuery) with suitable Cloud credentials and IAM; the Gemini API key does not
provide an authoritative spend-query endpoint. The Cloud Billing Budget API
manages budget configuration and notifications rather than serving as a
synchronous itemized spend ledger.

## 7. Spending limits and concurrency

Limits are optional exact-money values for the current UTC hour, calendar day,
and calendar month within one live session. `None` disables a period; zero
blocks immediately. Before each provider call, session spend is compared with
every enabled limit. A reached period returns
`Error::SpendingLimitReached` without contacting Google.

When any limit is active, clones serialize admission and the complete provider
call. This bounds concurrency overrun to the one admitted call because exact
usage is available only afterward. Limits do not coordinate separately opened
clients, other processes, or external calls, and they reset when the session is
dropped. They are a convenience guard, not an account-wide hard cap.

## 8. Managed-library conformance

The root has `Cargo.toml`, `Documentation.md`, `Version.txt`, ordinary UTF-8
source, a literal package version equal to `Version.txt`, and its own empty
`[workspace]` so it remains separate inside Kennedy's repository. Generated
build output, credentials, and binary media are not part of the managed
library. It is validated independently with formatting, build, Clippy, tests,
and documentation tests.
