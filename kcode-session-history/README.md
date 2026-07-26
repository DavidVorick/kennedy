# kcode-session-history

`kcode-session-history` is Kennedy's internal Rust library for durable session
history. It owns:

- the `kcode-session-log` dependency and every active session log;
- the opaque per-session `Session` handle used to mutate Chatend history;
- lifecycle checkpoints, ingress state, and browser-command journals;
- pending session objects; and
- the append-only index of completed Kweb session archives.

It deliberately has no HTTP framework, routes, multipart parsing, response
types, or KennedyServer dependency. KennedyServer owns those adapters and Kweb
commit policy.

## Boundary

The two primary handles are:

- `SessionHistory`: the long-lived store. Open it once, then use typed methods
  to start, register, list, checkpoint, transition, and complete sessions.
- `Session`: an opaque handle to one active session's ordered history,
  projections, and pending objects. It is created with a library-assigned ID
  or reopened through `SessionHistory`; callers never supply or receive
  storage paths.

There is no `SessionLease`. Kennedy already serializes writes where required,
and a second lifetime/locking abstraction would make the boundary larger
without adding an ownership guarantee that the application needs.

The crate is intentionally private to Kennedy (`publish = false`). Its
standalone manifest allows this directory to live in a separate repository and
be consumed through a Git dependency or a local development path.

See [Specification.md](Specification.md) for persistence and lifecycle
semantics.

## Verify

```sh
cargo test
```
