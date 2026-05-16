# amalgame-messaging-nats

NATS Core client for [Amalgame](https://github.com/amalgame-lang/Amalgame).
Pure-protocol implementation over raw TCP — no vendored client lib, no
`-lnats` at link time. Works against any NATS Core server: nats-server,
NGS (Synadia Cloud), self-hosted clusters.

## Install

```bash
amc package add nats                                              # via index
amc package add github.com/amalgame-lang/amalgame-messaging-nats@v0.1.0
```

Requires **amc 0.5.0+**.

## Surface

```amalgame
import Amalgame.Messaging.NATS

let n = NATS.Open("127.0.0.1", 4222)
if (!NATS.IsOpen(n)) {
    Console.WriteLine("connect failed: " + NATS.LastError(n))
    return
}

NATS.Subscribe(n, "greetings.>")
NATS.Publish(n, "greetings.hello", "world")

if (NATS.WaitMessage(n, 5000)) {
    Console.WriteLine(NATS.LastSubject(n) + " = " + NATS.LastPayload(n))
}

NATS.Close(n)
```

### v0.1.0 method surface

| Method | Returns | Notes |
|---|---|---|
| `NATS.Open(host, port)` | `AmalgameNATS*` | CONNECT + reads INFO; no auth |
| `NATS.Close(n)` | `void` | Closes socket cleanly |
| `NATS.IsOpen(n)` | `bool` | Connection alive? |
| `NATS.LastError(n)` | `string` | Empty on success |
| `NATS.Ping(n)` | `bool` | PING → PONG round-trip |
| `NATS.Publish(n, subject, payload)` | `bool` | PUB, fire-and-forget |
| `NATS.Subscribe(n, subject)` | `bool` | SUB with auto-assigned sid |
| `NATS.Unsubscribe(n, subject)` | `bool` | UNSUB the active sid |
| `NATS.WaitMessage(n, timeout_ms)` | `bool` | Blocks until MSG or timeout |
| `NATS.LastSubject(n)` | `string` | Subject of last received message |
| `NATS.LastPayload(n)` | `string` | Payload of last received message |
| `NATS.LastReplyTo(n)` | `string` | Reply-to subject (empty if none) |

### Subject syntax

NATS subjects are dot-separated tokens (`stocks.aapl.trades`).
Wildcards work server-side — pass them as part of the subject string:

- `*`  — single token  (`stocks.*.trades` matches `stocks.aapl.trades`)
- `>`  — rest of the subject (`stocks.>` matches `stocks.aapl.trades.live`)

The client just forwards the subject; matching happens on the server.

## Request / Reply

NATS Core's request/reply pattern is built on top of publish + a
unique reply-to subject. The v1 surface keeps these primitives
exposed but doesn't ship a `Request()` helper — assemble it yourself:

```amalgame
let inbox = "_INBOX." + Random.HexString(8)
NATS.Subscribe(n, inbox)
NATS.Publish(n, "service.echo", "ping")   // reply-to not yet exposed (v2)
if (NATS.WaitMessage(n, 2000)) {
    Console.WriteLine("got reply: " + NATS.LastPayload(n))
}
NATS.Unsubscribe(n, inbox)
```

Full `Request(subject, payload, timeout_ms)` lands in v2 alongside
explicit reply-to support on `Publish`.

## Deferred to v2

- Request/Reply convenience helper (`NATS.Request(subj, payload, timeout)`)
- Reply-to on `Publish`
- Multiple concurrent subscriptions tracked by sid
- Queue groups (`SUB <subj> <queue> <sid>`)
- JetStream (KV / Object store / streams) — separate package
- TLS (nats:// → nats+tls://)
- Username / password / token / NKEY / JWT auth
- Cluster discovery from INFO `connect_urls`
- Binary payloads (current code uses `code_string`; embedded NULs
  would truncate)

## Threading

`AmalgameNATS*` is single-owner. Don't call WaitMessage from one
thread while Publish runs on another against the same handle —
NATS is line-oriented and concurrent writers would interleave lines.
Different handles are safe.

## Tests

```bash
./tests/run_tests.sh /path/to/amc
```

The runner probes `127.0.0.1:4222` for a running NATS server; if
none is reachable every test SKIPs cleanly. Start a server locally
with `docker run --rm -p 4222:4222 nats:2-alpine` or
`brew install nats-server && nats-server &`.

## Licence

Apache-2.0. See [`LICENSE`](LICENSE) and [`NOTICE.md`](NOTICE.md).
