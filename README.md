# amalgame-messaging-nats

NATS Core client for [Amalgame](https://github.com/amalgame-lang/Amalgame).
Pure-protocol implementation over raw TCP — no vendored client lib, no
`-lnats` at link time. Works against any NATS Core server: nats-server,
NGS (Synadia Cloud), self-hosted clusters.

## Install

```bash
amc package add nats                                              # via index
amc package add github.com/amalgame-lang/amalgame-messaging-nats@v0.2.0
```

Requires **amc 0.8.19+**.

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

### v0.2.0 method surface

| Method | Returns | Notes |
|---|---|---|
| `NATS.Open(host, port)` | `AmalgameNATS*` | CONNECT + reads INFO; no auth |
| `NATS.Close(n)` | `void` | Closes socket cleanly |
| `NATS.IsOpen(n)` | `bool` | Connection alive? |
| `NATS.LastError(n)` | `string` | Empty on success |
| `NATS.Ping(n)` | `bool` | PING → PONG round-trip |
| `NATS.Publish(n, subject, payload)` | `bool` | PUB, fire-and-forget |
| `NATS.PublishWithReply(n, subject, replyTo, payload)` | `bool` | **v0.2** PUB with reply-to inbox |
| `NATS.Subscribe(n, subject)` | `bool` | SUB with auto-assigned sid |
| `NATS.SubscribeQueue(n, subject, queue)` | `bool` | **v0.2** SUB with queue group (server load-balances across members) |
| `NATS.Unsubscribe(n, subject)` | `bool` | UNSUB the active sid |
| `NATS.WaitMessage(n, timeout_ms)` | `bool` | Blocks until MSG or timeout |
| `NATS.LastSubject(n)` | `string` | Subject of last received message |
| `NATS.LastPayload(n)` | `string` | Payload of last received message |
| `NATS.LastReplyTo(n)` | `string` | Reply-to subject (empty if none) |
| `NATS.Request(n, subject, payload, timeout_ms)` | `string` | **v0.2** Synchronous request/reply — generates a unique inbox, SUBs, PUBs with reply-to, waits for the response, UNSUBs. Returns the response payload or `""` on timeout. |

### Subject syntax

NATS subjects are dot-separated tokens (`stocks.aapl.trades`).
Wildcards work server-side — pass them as part of the subject string:

- `*`  — single token  (`stocks.*.trades` matches `stocks.aapl.trades`)
- `>`  — rest of the subject (`stocks.>` matches `stocks.aapl.trades.live`)

The client just forwards the subject; matching happens on the server.

## Request / Reply (v0.2)

NATS Core's request/reply pattern is built on top of publish + a
unique reply-to subject. v0.2 ships a one-line helper:

```amalgame
let reply: string = NATS.Request(n, "service.echo", "ping", 2000)
if (String_Length(reply) > 0) {
    Console.WriteLine("got reply: " + reply)
} else {
    Console.WriteLine("timeout or no responder")
}
```

Under the hood `Request` generates a unique inbox (`_INBOX.<pid>.<counter>`),
SUBs on a dedicated sid (doesn't touch your user-side
`active_sid`), PUBs with the reply-to set, waits up to
`timeout_ms` for the first matching MSG, then UNSUBs. The
dedicated sid means concurrent Request calls on the same handle
don't collide (still serialize through the same wire — use
separate handles for true thread-parallel requests).

If you need to plumb the request/reply manually (e.g. multi-reply
patterns, or because you want to do other work between PUB and
WaitMessage), use `PublishWithReply` + your own Subscribe loop
instead — same primitives without the orchestration.

## Queue groups (v0.2)

```amalgame
NATS.SubscribeQueue(n, "work.requests", "workers")
```

Multiple subscribers with the same `queue` name form a group:
the server delivers each message to exactly one member of the
group, round-robin style. Used to spread incoming work across
multiple worker processes without external job scheduling.

## Deferred to v3

- Multiple concurrent user-side subscriptions tracked by sid
  (only `Request` creates a dedicated sid internally)
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
