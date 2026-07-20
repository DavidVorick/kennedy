# kcode-gemini Agent Integration Reference

`kcode-gemini` is a standalone asynchronous Rust 2024 library for the Gemini
features Kennedy uses. It owns Gemini transport, response normalization,
credential lifetime, current-session usage accounting, and live-session
spending-limit enforcement. The integrating server owns HTTP routes,
authentication, authorization, secret retrieval, request body limits, and
application policy.

The Cargo package is `kcode-gemini`; its Rust crate path is `kcode_gemini`.
Credential-bearing dependency versions and source-review conclusions are
recorded in `DependencyAudit.md`.

## Open a client

```rust,no_run
use kcode_gemini::{Gemini, Result};

fn open(api_key: String) -> Result<Gemini> {
    Gemini::open(api_key)
}
```

`Gemini::open` validates and takes ownership of the API key and starts with no
usage records or spending limits. The trimmed key is zeroized on final drop,
redacted from `Debug`, marked as a sensitive HTTP header value, sent only in
the `x-goog-api-key` request header, and never placed in a URL, request body,
accounting record, or returned error. The client disables environment proxy
discovery, redirects, automatic retries, and referrers, and allows HTTPS only.

Clones share the key, HTTP client, accounting state, and budget gate. Dropping
the last clone discards every usage record and configured limit. There is no
database path, persistence mode, SQLite integration, file output, or recovery
behavior.

## Inference methods

```rust,no_run
use kcode_gemini::{
    Gemini, InferenceRequest, MediaInput, MultimodalRequest,
    NanoBananaProRequest, Result,
};

async fn examples(gemini: &Gemini, png: Vec<u8>, wav: Vec<u8>) -> Result<()> {
    let fast = gemini
        .infer_flash_lite(InferenceRequest::new("Summarize this note."))
        .await?;
    println!("{}", fast.text.as_deref().unwrap_or(""));

    let pro = gemini
        .infer_pro(InferenceRequest::new("Solve this carefully."))
        .await?;
    println!("{}", pro.text.as_deref().unwrap_or(""));

    let multimodal = MultimodalRequest::new(
        "Compare what is visible with what is said.",
        vec![
            MediaInput::image("image/png", png.clone())?,
            MediaInput::audio("audio/wav", wav)?,
        ],
    );
    gemini.infer_pro_multimodal(multimodal).await?;

    let mut image =
        NanoBananaProRequest::new("Turn this into a clean product photo.");
    image.images.push(MediaInput::image("image/png", png)?);
    let generated = gemini.nano_banana_pro(image).await?;
    assert!(!generated.images.is_empty());
    Ok(())
}
```

| Method | Fixed model | Inputs | Output |
|---|---|---|---|
| `infer_flash_lite` | `gemini-3.1-flash-lite` | text | text |
| `infer_pro` | `gemini-3.1-pro-preview` | text | text |
| `infer_pro_multimodal` | `gemini-3.1-pro-preview` | text plus image and/or audio | text |
| `nano_banana_pro` | `gemini-3-pro-image` | text plus optional images | one 2K image, optional text |
| `grounded_search` | `gemini-3.1-flash-lite` | text plus Google Search | text and normalized sources |

There is deliberately no Gemini 3.5 Flash or Nano Banana 2 method. Arbitrary
model names are not accepted.

Media is base64-encoded into Interactions API content blocks and is not
uploaded to Google File storage. Aggregate raw inline media is capped at 64
MiB. Media and generated-image `Debug` output reports lengths without bytes.
Nano Banana Pro accepts up to 14 reference images and supports its documented
2K aspect ratios. Output resolution is not a caller option: every image request
sends `image_size: "2K"`.

Every inference request uses `store: false`. `GenerationOptions` controls the
output cap, optional temperature, optional thinking level, and Standard or
Priority service tier. Nano Banana Pro is restricted to Standard service in
this crate. Flash-Lite defaults to the model's native thinking behavior; Pro
rejects unsupported `minimal` thinking. `GroundedSearchRequest::new` preserves
Kennedy's low-thinking, 2048-token, Priority-tier fast-search policy.

## Usage and spending

```rust,no_run
use kcode_gemini::{Gemini, Money, Result, SpendingLimits, UsageWindow};

fn accounting(gemini: &Gemini) -> Result<()> {
    gemini.set_spending_limits(SpendingLimits {
        hourly: Some(Money::from_usd_micros(2_000_000)), // $2
        daily: Some(Money::from_usd_micros(10_000_000)), // $10
        monthly: Some(Money::from_usd_micros(100_000_000)), // $100
    })?;

    let month = gemini.usage_breakdown(UsageWindow::CurrentMonth)?;
    println!("session month estimate: {}", month.totals.cost.total);
    let recent = gemini.usage_records(UsageWindow::CurrentDay, 100)?;
    println!("{} session attempts today", recent.len());
    Ok(())
}
```

`usage_breakdown` returns current-session request success/failure counts, token
totals, input/cached/output modality splits, thought/tool/search usage, cost
components, groups by exact model, groups by operation, and current limit
status. `usage_records` returns newest-first individual session records.

These are ordinary typed Rust methods and values. The crate does not include
an HTTP endpoint, router, route-ready serialization layer, or prescribed wire
format. `kennedy-server` can later choose whether and how to expose them.

Costs are integer USD nanos calculated from the compiled Google Gemini
Developer API prices. Missing optional modality detail produces
`CostAccuracy::Estimated`. Grounding produces `CostAccuracy::Conservative`
because the calculation charges every reported search query and cannot observe
Google's shared free pool. Nano Banana Pro 1K/2K image output is charged from
the provider-reported image-token count at the published Pro image-output rate.

This is not an authoritative account ledger. It cannot see usage through other
client instances, processes, API keys, projects, or tools, and it cannot see
credits, taxes, negotiated pricing, delayed adjustments, or future price
changes. Google AI Studio and Cloud Billing remain authoritative. Querying
actual Cloud Billing cost programmatically requires separately configured
Cloud Billing export data and Google Cloud credentials; a Gemini API key alone
does not provide it.

Limits use UTC clock hours, UTC calendar days, and UTC calendar months within
the live session. Once recorded session spend is at or above a configured
limit, the next provider request returns `Error::SpendingLimitReached`. Calls
are serialized while any limit is active so clones cannot create simultaneous
one-request overruns. One admitted request can cross a limit because its usage
is known only after Gemini responds. A newly opened client has zero spend and
no limits.

## Managed-library maintenance

`Version.txt` is the canonical managed-library version and must match the
literal root package version in `Cargo.toml`. All maintained files are ordinary
UTF-8 text. Keep `target/`, credentials, media, and generated or binary files
outside the managed library. The standard kcode check performs fetch,
formatting, build, Clippy with warnings denied, tests, and doc tests.
