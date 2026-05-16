# NOTICE — amalgame-messaging-nats

## Authorship

Copyright 2026 Bastien Mouget. Original work — see
`runtime/Amalgame_Messaging_NATS.h`.

Part of the Amalgame ecosystem
([github.com/amalgame-lang/Amalgame](https://github.com/amalgame-lang/Amalgame)).
External contributions are paused at the ecosystem level; see the
main repo's `CONTRIBUTING.md` for the policy.

AI tools (Anthropic Claude) were used during development. Per
the project's authorship policy, AI is treated as a tool, not a
co-author at law.

## Licence

Apache License 2.0. See `LICENSE` for the full text.

## Third-party content

**None vendored.** This package implements the NATS Core wire
protocol directly in ~500 LoC of C against the cross-platform
socket layer that amc already ships. There is no `libnats.c` /
`cnats` / NATS-server dependency at link time, no embedded fork
of upstream NATS client code.

The [NATS protocol specification](https://docs.nats.io/reference/reference-protocols/nats-protocol)
is published under the Apache-2.0 licence by the NATS Authors
([github.com/nats-io](https://github.com/nats-io)).

## Trademarks

"NATS" is a trademark of Synadia Communications Inc. (custodian
of the open-source NATS project). This repository uses the name
solely to identify the protocol being implemented. No trademark
claim is asserted.
