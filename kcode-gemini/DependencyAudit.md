# Credential-Bearing Dependency Audit

Audit date: 2026-07-20 UTC.

This record covers the exact third-party crates that directly receive or retain
the Gemini API key through `kcode-gemini` APIs. Any version change invalidates
the corresponding conclusion and requires a fresh source audit before use.

## reqwest 0.13.4

- Crates.io archive SHA-256:
  `219c5811de6525e5416c7d5d53bb656d3afdbc6c5af816e0802bcfa42dbdc1c3`.
- Enabled features: `json` and `rustls`; default features are disabled.
- Reviewed material: the complete published archive, including both manifests,
  every `src/**/*.rs` module, licenses, repository metadata, and all unsafe,
  environment, filesystem, logging, redirect, retry, proxy, request-debug,
  header, TLS, and outbound-network paths. The crate has no build script.
- Findings: no telemetry or autonomous destination exists. Runtime network
  destinations come from caller requests or explicitly configured proxies.
  Request `Debug` includes headers, so `kcode-gemini` supplies the API key as a
  `HeaderValue` marked sensitive. Reqwest can discover proxies, follow
  redirects, send referrers, retry requests, or emit wire bytes when explicitly
  configured; `kcode-gemini` disables all five behaviors and enables HTTPS-only
  mode. Ordinary logs contain connection/retry metadata, not request headers or
  bodies. Error values can retain request URLs but not header values; the
  library additionally reduces transport errors to fixed classifications.
  Filesystem-capable blocking/multipart APIs, cookies, system-proxy support,
  compression, HTTP/2, HTTP/3, and verbose wire logging are not enabled or used.
- Conclusion: acceptable for passing the API key solely to the fixed Google
  Interactions endpoint under the client configuration above.

## zeroize 1.9.0

- Crates.io archive SHA-256:
  `e13c156562582aa81c60cb29407084cdb54c4164760106ab78e6c5b0858cf64e`.
- Enabled features: default `alloc`; derive and Serde support are not enabled.
- Reviewed material: the complete published archive, including both manifests,
  every source and test file, licenses, repository metadata, architecture
  barriers, volatile writes, collection/string implementations, and unsafe
  blocks. The crate has no build script.
- Findings: the crate performs no network, filesystem, environment, logging,
  telemetry, process, or dynamic-code operations. `Zeroizing<String>` invokes
  the audited `String`/`Vec` implementation on drop, overwriting initialized and
  spare capacity before clearing the allocation. The crate documents that
  reallocations and compiler-created copies cannot be retroactively erased.
- Conclusion: acceptable for best-effort in-memory clearing of the owned API
  key, with the documented limits of software zeroization.

## Re-audit rule

Both versions are exact-pinned in `Cargo.toml` and checksum-pinned by
`Cargo.lock`. Before changing either exact version, compare and review the full
new published source and replace this record. Automated vulnerability scanning
may supplement but cannot replace that review.
