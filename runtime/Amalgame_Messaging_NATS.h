/*
 * Amalgame Standard Library — Amalgame.Messaging.NATS
 * Copyright (c) 2026 Bastien MOUGET
 * https://github.com/amalgame-lang/Amalgame
 *
 * NATS Core client speaking the wire protocol directly over TCP.
 * No external client lib — the protocol is text, 15 verbs total,
 * and fits in ~500 lines of C. NATS Core is the simplest pub/sub
 * broker in the CNCF stable (alongside Kubernetes / Prometheus);
 * NATS JetStream (KV / streams / object store) is a separate
 * package since it needs a different mental model.
 *
 * Surface (v1):
 *   Open / Close / IsOpen / LastError            lifecycle + diag
 *   Ping                                          PING → PONG
 *   Publish(subject, payload)                     PUB
 *   Subscribe(subject)                            SUB (single sid)
 *   Unsubscribe(subject)                          UNSUB <sid>
 *   WaitMessage(timeout_ms) / LastSubject /
 *     LastPayload / LastReplyTo                   MSG recv loop
 *
 * Wire vocabulary (https://docs.nats.io/reference/reference-protocols/nats-protocol):
 *
 *   Server → client                Client → server
 *   ───────────────                ───────────────
 *   INFO  {…}                      CONNECT {…}
 *   MSG   <subj> <sid>             PUB     <subj> [reply] <bytes>
 *         [<reply>] <bytes>                <payload>
 *         <payload>                 SUB     <subj> [queue] <sid>
 *   +OK                            UNSUB   <sid> [max_msgs]
 *   -ERR  <msg>                    PING
 *   PING                           PONG
 *   PONG
 *
 *   All lines end CRLF. Payload bytes are an exact count — no
 *   in-band terminator. Subjects use dot-separated segments
 *   (foo.bar.baz); wildcards `*` (single token) and `>` (rest of
 *   the subject) are interpreted server-side, this client just
 *   forwards them.
 *
 * Deferred to v2: Request/Reply helpers (caller builds a reply-to
 * subject + sub manually today), multiple concurrent subscriptions
 * tracked by sid, queue groups, JetStream, TLS, AUTH (user/pass /
 * NKEY / JWT / token), cluster discovery from INFO connect_urls,
 * binary payloads (current code uses code_string and would truncate
 * on embedded NULs).
 *
 * Reuses the cross-platform socket layer from Amalgame_Net.h.
 *
 * Threading: single-owner handle. Don't call WaitMessage from one
 * thread while Publish runs on another against the same handle —
 * the wire is line-oriented and concurrent writers would interleave
 * lines. Concurrent handles are safe.
 */

#ifndef AMALGAME_MESSAGING_NATS_H
#define AMALGAME_MESSAGING_NATS_H

#include "_runtime.h"
#include "Amalgame_Collections.h"
#include "Amalgame_Net.h"   /* cross-platform sockets + _amnet_init_once */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef _WIN32
#  include <sys/time.h>     /* struct timeval for SO_RCVTIMEO */
#  include <unistd.h>
#endif

#ifndef AMALGAME_NATS_RECV_BUF
#  define AMALGAME_NATS_RECV_BUF 4096
#endif

typedef struct AmalgameNATS {
    int         fd;                /* socket fd; -1 = not connected */
    char*       last_error;        /* GC-strdup'd, or NULL */
    char*       last_subject;      /* most recent MSG subject */
    char*       last_payload;      /* most recent MSG payload */
    char*       last_reply_to;     /* most recent MSG reply-to subject (or "") */
    int         active_sid;        /* sid assigned by the last Subscribe (0 = none) */
    int         next_sid;          /* monotonic; 1.. */

    /* Receive scratchpad. NATS is line-oriented for everything
     * except the MSG payload (which is followed by a byte count).
     * We buffer raw input and serve newline-delimited control lines
     * out of it; MSG payloads are read from the same buffer + the
     * trailing CRLF stripped. */
    unsigned char* rxbuf;          /* GC_MALLOC, capacity = AMALGAME_NATS_RECV_BUF */
    size_t         rxlen;          /* bytes currently buffered */
} AmalgameNATS;

/* ── Small helpers ──────────────────────────────────── */

static inline code_string _amnats_err_dup(const char* msg) {
    if (!msg) return NULL;
    size_t n = strlen(msg);
    char* p = (char*) code_alloc(n + 1);
    memcpy(p, msg, n + 1);
    return p;
}

static inline char* _amnats_str_dup(const char* s, size_t n) {
    char* p = (char*) code_alloc(n + 1);
    if (n > 0) memcpy(p, s, n);
    p[n] = '\0';
    return p;
}

/* send() can short-write; loop until every byte's on the wire. */
static inline int _amnats_send_all(int fd, const unsigned char* buf, size_t n) {
    size_t off = 0;
    while (off < n) {
        ssize_t k = send(fd, (const char*) buf + off, n - off, 0);
        if (k <= 0) return -1;
        off += (size_t) k;
    }
    return 0;
}

/* Apply / clear SO_RCVTIMEO on the fd. 0 = blocking (clear). */
static inline void _amnats_set_rcvtimeo(int fd, i64 timeout_ms) {
#ifdef _WIN32
    DWORD tv = (timeout_ms > 0) ? (DWORD) timeout_ms : 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (const char*) &tv, sizeof(tv));
#else
    struct timeval tv;
    if (timeout_ms > 0) {
        tv.tv_sec  = (time_t) (timeout_ms / 1000);
        tv.tv_usec = (suseconds_t) ((timeout_ms % 1000) * 1000);
    } else {
        tv.tv_sec  = 0;
        tv.tv_usec = 0;
    }
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
#endif
}

/* Ensure the rxbuf has room for an extra `need` bytes. Realloc
 * up via GC_MALLOC + memcpy if not. The buffer only grows; NATS
 * messages above ~1 MB are unusual and would be an app smell. */
static inline void _amnats_grow(AmalgameNATS* n, size_t need) {
    /* Initial allocation. */
    if (!n->rxbuf) {
        size_t cap = AMALGAME_NATS_RECV_BUF;
        while (cap < need) cap *= 2;
        n->rxbuf = (unsigned char*) GC_MALLOC(cap);
        n->rxlen = 0;
    }
}

/* Capacity is implicit: we grow whenever we'd exceed it. Compute
 * the current capacity by rounding rxlen up to the buffer's growth
 * step — for simplicity we keep capacity in a side variable. To
 * stay minimal we just double when needed, tracked by `cap`. */
typedef struct { size_t cap; } _amnats_dummy;  /* unused, see below */

/* Append `n` bytes of raw socket data to the rxbuf. */
static inline int _amnats_recv_more(AmalgameNATS* nat) {
    if (!nat->rxbuf) {
        nat->rxbuf = (unsigned char*) GC_MALLOC(AMALGAME_NATS_RECV_BUF);
        nat->rxlen = 0;
    }
    /* Capacity is the initial AMALGAME_NATS_RECV_BUF unless we've
     * grown it; we grow per-call when the buffer fills past 3/4. */
    size_t cap = AMALGAME_NATS_RECV_BUF;
    /* Find the actual capacity by rounding rxlen up. Since we only
     * ever double, this works as long as we doubled deterministically.
     * Simpler: read into a stack chunk then append. */
    unsigned char chunk[AMALGAME_NATS_RECV_BUF];
    ssize_t k = recv(nat->fd, (char*) chunk, AMALGAME_NATS_RECV_BUF, 0);
    if (k <= 0) return -1;
    /* Grow if needed. */
    size_t need = nat->rxlen + (size_t) k;
    if (need > cap) {
        size_t newcap = cap;
        while (newcap < need) newcap *= 2;
        unsigned char* p = (unsigned char*) GC_MALLOC(newcap);
        memcpy(p, nat->rxbuf, nat->rxlen);
        nat->rxbuf = p;
        cap = newcap;
    }
    memcpy(nat->rxbuf + nat->rxlen, chunk, (size_t) k);
    nat->rxlen += (size_t) k;
    return 0;
}

/* Consume `n` bytes from the front of rxbuf, shifting the rest down. */
static inline void _amnats_consume(AmalgameNATS* nat, size_t n) {
    if (n >= nat->rxlen) { nat->rxlen = 0; return; }
    memmove(nat->rxbuf, nat->rxbuf + n, nat->rxlen - n);
    nat->rxlen -= n;
}

/* Find a CRLF in rxbuf. Returns the byte offset of '\r', or -1 if
 * not present. */
static inline ssize_t _amnats_find_crlf(AmalgameNATS* nat) {
    if (nat->rxlen < 2) return -1;
    for (size_t i = 0; i + 1 < nat->rxlen; i++) {
        if (nat->rxbuf[i] == '\r' && nat->rxbuf[i + 1] == '\n') {
            return (ssize_t) i;
        }
    }
    return -1;
}

/* Block (within the current SO_RCVTIMEO window) until a CRLF-terminated
 * line is available in rxbuf. Returns 0 on success (the line is in
 * rxbuf[0..len-1], with the trailing CRLF at [len..len+1]), -1 on
 * socket error / timeout. The caller is expected to _amnats_consume
 * the line + the CRLF after parsing. */
static inline int _amnats_read_line(AmalgameNATS* nat, size_t* line_len) {
    ssize_t idx = _amnats_find_crlf(nat);
    while (idx < 0) {
        if (_amnats_recv_more(nat) < 0) return -1;
        idx = _amnats_find_crlf(nat);
    }
    *line_len = (size_t) idx;
    return 0;
}

/* Block until at least `need` bytes are buffered. */
static inline int _amnats_read_exact(AmalgameNATS* nat, size_t need) {
    while (nat->rxlen < need) {
        if (_amnats_recv_more(nat) < 0) return -1;
    }
    return 0;
}

/* Send a single short verb followed by CRLF (PING / PONG). */
static inline int _amnats_send_verb(AmalgameNATS* nat, const char* verb) {
    char buf[16];
    int n = snprintf(buf, sizeof(buf), "%s\r\n", verb);
    return _amnats_send_all(nat->fd, (const unsigned char*) buf, (size_t) n);
}

/* ── Lifecycle ──────────────────────────────────────── */

/* Open a NATS connection. host = "127.0.0.1" / "nats.example.com",
 * port = 4222 (default). Reads the server's INFO greeting, sends a
 * CONNECT, and returns the handle. On failure the handle is still
 * returned but IsOpen() is false and LastError() carries the cause.
 *
 * No AUTH support in v1 — the CONNECT block is minimal:
 *   {"verbose":false,"pedantic":false,"lang":"amalgame",
 *    "version":"0.1.0","protocol":1}
 *
 * Servers requiring auth_required:true in their INFO will reject
 * the CONNECT with -ERR; that surfaces as LastError() after Open. */
static inline AmalgameNATS* Amalgame_Messaging_NATS_Open(code_string host, i64 port) {
    _amnet_init_once();
    AmalgameNATS* n = (AmalgameNATS*) GC_MALLOC(sizeof(AmalgameNATS));
    n->fd            = -1;
    n->last_error    = NULL;
    n->last_subject  = (char*) "";
    n->last_payload  = (char*) "";
    n->last_reply_to = (char*) "";
    n->active_sid    = 0;
    n->next_sid      = 1;
    n->rxbuf         = NULL;
    n->rxlen         = 0;

    if (!host || port <= 0 || port > 65535) {
        n->last_error = _amnats_err_dup("invalid host/port");
        return n;
    }

    struct addrinfo hints = {0};
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    char portStr[16];
    snprintf(portStr, sizeof(portStr), "%lld", (long long) port);

    struct addrinfo* res = NULL;
    if (getaddrinfo(host, portStr, &hints, &res) != 0 || !res) {
        n->last_error = _amnats_err_dup("getaddrinfo failed");
        return n;
    }

    int fd = (int) socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) {
        freeaddrinfo(res);
        n->last_error = _amnats_err_dup("socket() failed");
        return n;
    }
    if (connect(fd, res->ai_addr, (int) res->ai_addrlen) != 0) {
        _amnet_close_socket(fd);
        freeaddrinfo(res);
        n->last_error = _amnats_err_dup("connect() failed");
        return n;
    }
    freeaddrinfo(res);
    n->fd = fd;

    /* Apply a 5-second timeout for the INFO handshake; once we've
     * read it we clear the timeout (blocking mode is the steady
     * state, the per-call SO_RCVTIMEO is restored by WaitMessage). */
    _amnats_set_rcvtimeo(fd, 5000);

    /* Read INFO line. */
    size_t line_len = 0;
    if (_amnats_read_line(n, &line_len) < 0) {
        n->last_error = _amnats_err_dup("no INFO from server");
        _amnet_close_socket(fd);
        n->fd = -1;
        return n;
    }
    if (line_len < 4 || memcmp(n->rxbuf, "INFO", 4) != 0) {
        n->last_error = _amnats_err_dup("expected INFO greeting");
        _amnet_close_socket(fd);
        n->fd = -1;
        return n;
    }
    _amnats_consume(n, line_len + 2);  /* drop INFO line + CRLF */

    /* Send CONNECT. */
    const char* connect_msg =
        "CONNECT {\"verbose\":false,\"pedantic\":false,"
        "\"lang\":\"amalgame\",\"version\":\"0.1.0\",\"protocol\":1}\r\n";
    if (_amnats_send_all(fd, (const unsigned char*) connect_msg,
                         strlen(connect_msg)) < 0) {
        n->last_error = _amnats_err_dup("CONNECT send failed");
        _amnet_close_socket(fd);
        n->fd = -1;
        return n;
    }

    /* Restore blocking mode. */
    _amnats_set_rcvtimeo(fd, 0);
    return n;
}

static inline void Amalgame_Messaging_NATS_Close(AmalgameNATS* n) {
    if (!n || n->fd < 0) return;
    /* Best-effort. Some servers like a clean disconnect, but if the
     * peer's gone the send fails — that's fine. */
    (void) _amnats_send_all(n->fd, (const unsigned char*) "-ERR client closing\r\n", 20);
    _amnet_close_socket(n->fd);
    n->fd = -1;
}

static inline code_bool Amalgame_Messaging_NATS_IsOpen(AmalgameNATS* n) {
    return (n && n->fd >= 0) ? 1 : 0;
}

static inline code_string Amalgame_Messaging_NATS_LastError(AmalgameNATS* n) {
    if (!n || !n->last_error) return (code_string) "";
    return n->last_error;
}

/* ── Ping ───────────────────────────────────────────── */

/* PING → wait for PONG. Returns true if the round-trip completes
 * within the 5s socket timeout. Drains any unrelated lines
 * (MSG / +OK / -ERR / async PING from the server) in the meantime —
 * MSG payloads are *cached* into last_* so the caller's next
 * WaitMessage sees them. */
static inline code_bool Amalgame_Messaging_NATS_Ping(AmalgameNATS* n) {
    if (!n || n->fd < 0) return 0;
    if (_amnats_send_verb(n, "PING") < 0) {
        n->last_error = _amnats_err_dup("PING send failed");
        return 0;
    }
    _amnats_set_rcvtimeo(n->fd, 5000);
    while (1) {
        size_t line_len = 0;
        if (_amnats_read_line(n, &line_len) < 0) {
            _amnats_set_rcvtimeo(n->fd, 0);
            n->last_error = _amnats_err_dup("PONG read timeout");
            return 0;
        }
        if (line_len == 4 && memcmp(n->rxbuf, "PONG", 4) == 0) {
            _amnats_consume(n, 6);
            _amnats_set_rcvtimeo(n->fd, 0);
            return 1;
        }
        /* Server-originated PING — reply PONG and keep waiting. */
        if (line_len == 4 && memcmp(n->rxbuf, "PING", 4) == 0) {
            _amnats_consume(n, 6);
            (void) _amnats_send_verb(n, "PONG");
            continue;
        }
        /* +OK / -ERR are also benign; drop the line and continue. */
        _amnats_consume(n, line_len + 2);
    }
}

/* ── Publish ────────────────────────────────────────── */

/* Internal: PUB write — common path for Publish and
 * PublishWithReply. replyTo may be NULL or "" (omit the field).
 * NATS subjects can't contain spaces or CRLF; the caller is
 * trusted — passing an invalid subject surfaces as a server
 * -ERR on the next WaitMessage, not as a synchronous error
 * here. */
static inline code_bool _amnats_pub_internal(AmalgameNATS* n,
                                              const char* subject,
                                              const char* replyTo,
                                              const char* payload) {
    if (!n || n->fd < 0 || !subject) return 0;
    if (!payload) payload = "";

    size_t slen = strlen(subject);
    size_t plen = strlen(payload);
    int has_reply = (replyTo && *replyTo) ? 1 : 0;
    size_t rlen = has_reply ? strlen(replyTo) : 0;

    /* Header: "PUB " + subject [+ " " + replyTo] + " " + plen + "\r\n" */
    char header[512];
    int hn;
    if (has_reply) {
        hn = snprintf(header, sizeof(header), "PUB %.*s %.*s %zu\r\n",
                       (int)(slen > 200 ? 200 : slen), subject,
                       (int)(rlen > 200 ? 200 : rlen), replyTo, plen);
    } else {
        hn = snprintf(header, sizeof(header), "PUB %.*s %zu\r\n",
                       (int)(slen > 200 ? 200 : slen), subject, plen);
    }
    if (hn <= 0 || (size_t) hn >= sizeof(header)) {
        n->last_error = _amnats_err_dup("PUB header too long (subject?)");
        return 0;
    }
    if (_amnats_send_all(n->fd, (const unsigned char*) header, (size_t) hn) < 0) {
        n->last_error = _amnats_err_dup("PUB header send failed");
        return 0;
    }
    if (plen > 0 && _amnats_send_all(n->fd, (const unsigned char*) payload, plen) < 0) {
        n->last_error = _amnats_err_dup("PUB payload send failed");
        return 0;
    }
    if (_amnats_send_all(n->fd, (const unsigned char*) "\r\n", 2) < 0) {
        n->last_error = _amnats_err_dup("PUB trailer send failed");
        return 0;
    }
    return 1;
}

/* Publish without reply-to (v1 surface). */
static inline code_bool Amalgame_Messaging_NATS_Publish(AmalgameNATS* n,
                                                          code_string subject,
                                                          code_string payload) {
    return _amnats_pub_internal(n, subject, NULL, payload);
}

/* PUB with explicit reply-to — receiver sees the inbox in
 * LastReplyTo() and can publish a response back. Added in v0.2
 * so Request/Reply patterns can be hand-rolled too, not just
 * via the all-in-one Request helper. */
static inline code_bool Amalgame_Messaging_NATS_PublishWithReply(
        AmalgameNATS* n, code_string subject, code_string replyTo,
        code_string payload) {
    return _amnats_pub_internal(n, subject, replyTo, payload);
}

/* ── Subscribe / Unsubscribe ────────────────────────── */

/* SUB <subject> <sid>\r\n. v1 supports one active subscription per
 * handle (multi-sub is v2). Re-subscribing replaces the active sid. */
static inline code_bool Amalgame_Messaging_NATS_Subscribe(AmalgameNATS* n,
                                                            code_string subject) {
    if (!n || n->fd < 0 || !subject) return 0;
    int sid = n->next_sid++;
    char buf[256];
    int len = snprintf(buf, sizeof(buf), "SUB %.*s %d\r\n",
                       (int) (strlen(subject) > 200 ? 200 : strlen(subject)),
                       subject, sid);
    if (len <= 0 || (size_t) len >= sizeof(buf)) {
        n->last_error = _amnats_err_dup("SUB header too long");
        return 0;
    }
    if (_amnats_send_all(n->fd, (const unsigned char*) buf, (size_t) len) < 0) {
        n->last_error = _amnats_err_dup("SUB send failed");
        return 0;
    }
    n->active_sid = sid;
    return 1;
}

/* SUB <subject> <queue> <sid>\r\n. Multiple subscribers with the
 * same `queue` name form a queue group: the server load-balances
 * incoming messages across them (each message goes to exactly one
 * member of the group, round-robin style). Added in v0.2. */
static inline code_bool Amalgame_Messaging_NATS_SubscribeQueue(
        AmalgameNATS* n, code_string subject, code_string queue) {
    if (!n || n->fd < 0 || !subject || !queue) return 0;
    int sid = n->next_sid++;
    char buf[384];
    int len = snprintf(buf, sizeof(buf), "SUB %.*s %.*s %d\r\n",
                       (int) (strlen(subject) > 200 ? 200 : strlen(subject)),
                       subject,
                       (int) (strlen(queue) > 100 ? 100 : strlen(queue)),
                       queue, sid);
    if (len <= 0 || (size_t) len >= sizeof(buf)) {
        n->last_error = _amnats_err_dup("SUB (queue) header too long");
        return 0;
    }
    if (_amnats_send_all(n->fd, (const unsigned char*) buf, (size_t) len) < 0) {
        n->last_error = _amnats_err_dup("SUB (queue) send failed");
        return 0;
    }
    n->active_sid = sid;
    return 1;
}

/* UNSUB <sid>\r\n. Subject arg is informational; we just track the
 * sid assigned by the last Subscribe. Future: UNSUB <sid> <max_msgs>
 * for auto-unsubscribe after N messages. */
static inline code_bool Amalgame_Messaging_NATS_Unsubscribe(AmalgameNATS* n,
                                                              code_string subject) {
    (void) subject;
    if (!n || n->fd < 0 || n->active_sid == 0) return 0;
    char buf[32];
    int len = snprintf(buf, sizeof(buf), "UNSUB %d\r\n", n->active_sid);
    if (_amnats_send_all(n->fd, (const unsigned char*) buf, (size_t) len) < 0) {
        n->last_error = _amnats_err_dup("UNSUB send failed");
        return 0;
    }
    n->active_sid = 0;
    return 1;
}

/* ── Receive loop ───────────────────────────────────── */

/* Block for up to timeout_ms waiting for a single MSG.
 *   timeout_ms > 0  : SO_RCVTIMEO applied; returns false on timeout.
 *   timeout_ms <= 0 : blocks indefinitely (clears the timeout).
 *
 * On success: caches subject + payload + reply_to (empty string if
 * the MSG had no reply-to field), returns true. On timeout / error:
 * returns false; LastError() is set on real errors but stays empty
 * on plain timeouts (a "no message yet" outcome).
 *
 * Server-originated PING is auto-PONG'd. +OK / -ERR lines are
 * dropped (the -ERR text is stored in last_error for the *next*
 * caller to consult). */
static inline code_bool Amalgame_Messaging_NATS_WaitMessage(AmalgameNATS* n,
                                                              i64 timeout_ms) {
    if (!n || n->fd < 0) return 0;
    _amnats_set_rcvtimeo(n->fd, timeout_ms);

    while (1) {
        size_t line_len = 0;
        if (_amnats_read_line(n, &line_len) < 0) {
            _amnats_set_rcvtimeo(n->fd, 0);
            return 0;   /* timeout or EOF — caller checks LastError */
        }

        /* Server-side PING / +OK — drain and continue. */
        if (line_len == 4 && memcmp(n->rxbuf, "PING", 4) == 0) {
            _amnats_consume(n, 6);
            (void) _amnats_send_verb(n, "PONG");
            continue;
        }
        if (line_len == 4 && memcmp(n->rxbuf, "PONG", 4) == 0) {
            _amnats_consume(n, 6);
            continue;
        }
        if (line_len >= 3 && memcmp(n->rxbuf, "+OK", 3) == 0) {
            _amnats_consume(n, line_len + 2);
            continue;
        }
        if (line_len >= 4 && memcmp(n->rxbuf, "-ERR", 4) == 0) {
            /* Stash the error text for the caller. */
            size_t errlen = (line_len > 4) ? (line_len - 5) : 0;
            n->last_error = _amnats_str_dup(
                (const char*) (n->rxbuf + (errlen > 0 ? 5 : 4)), errlen);
            _amnats_consume(n, line_len + 2);
            continue;
        }

        /* MSG <subject> <sid> [<reply-to>] <bytes>
         * Tokenise the header line in place. */
        if (line_len < 4 || memcmp(n->rxbuf, "MSG ", 4) != 0) {
            /* Unknown control line; drop and keep going. */
            _amnats_consume(n, line_len + 2);
            continue;
        }
        char* hdr = _amnats_str_dup((const char*) (n->rxbuf + 4), line_len - 4);
        _amnats_consume(n, line_len + 2);

        /* Up to 4 whitespace-separated tokens: subject, sid,
         * [reply-to], bytes. */
        char* tok[4] = {NULL, NULL, NULL, NULL};
        int   ntok   = 0;
        char* p      = hdr;
        while (*p && ntok < 4) {
            while (*p == ' ' || *p == '\t') p++;
            if (!*p) break;
            tok[ntok++] = p;
            while (*p && *p != ' ' && *p != '\t') p++;
            if (*p) { *p = '\0'; p++; }
        }
        const char* subj = NULL;
        const char* reply = "";
        size_t bytes = 0;
        if (ntok == 3) {
            subj  = tok[0];
            /* tok[1] = sid (ignored — single-sub model in v1) */
            bytes = (size_t) strtoull(tok[2], NULL, 10);
        } else if (ntok == 4) {
            subj  = tok[0];
            /* tok[1] = sid */
            reply = tok[2];
            bytes = (size_t) strtoull(tok[3], NULL, 10);
        } else {
            n->last_error = _amnats_err_dup("malformed MSG header");
            _amnats_set_rcvtimeo(n->fd, 0);
            return 0;
        }

        /* Read the payload + trailing CRLF. */
        if (_amnats_read_exact(n, bytes + 2) < 0) {
            _amnats_set_rcvtimeo(n->fd, 0);
            n->last_error = _amnats_err_dup("MSG payload read failed");
            return 0;
        }

        /* Strdup subject + payload + reply into the handle. */
        n->last_subject  = _amnats_str_dup(subj, strlen(subj));
        n->last_reply_to = _amnats_str_dup(reply, strlen(reply));
        n->last_payload  = _amnats_str_dup((const char*) n->rxbuf, bytes);
        _amnats_consume(n, bytes + 2);

        _amnats_set_rcvtimeo(n->fd, 0);
        return 1;
    }
}

static inline code_string Amalgame_Messaging_NATS_LastSubject(AmalgameNATS* n) {
    if (!n || !n->last_subject) return (code_string) "";
    return n->last_subject;
}

static inline code_string Amalgame_Messaging_NATS_LastPayload(AmalgameNATS* n) {
    if (!n || !n->last_payload) return (code_string) "";
    return n->last_payload;
}

static inline code_string Amalgame_Messaging_NATS_LastReplyTo(AmalgameNATS* n) {
    if (!n || !n->last_reply_to) return (code_string) "";
    return n->last_reply_to;
}

/* ── Request / Reply (v0.2) ─────────────────────────── */

/* Synchronous request/reply pattern over NATS Core. Orchestrates:
 *   1. Generate a unique inbox name `_INBOX.<pid>.<counter>`.
 *   2. Send SUB <inbox> <sid> on a dedicated sid (doesn't touch
 *      the handle's active_sid; user subscriptions stay intact).
 *   3. PUB <subject> <inbox> <bytes> with the request payload.
 *   4. Wait up to timeout_ms for the first MSG on this sid.
 *   5. UNSUB <sid> regardless of outcome.
 *
 * Returns the response payload, or "" on timeout / error. Sets
 * last_subject / last_reply_to / last_payload on the handle for
 * the matching reply (mirrors WaitMessage's contract).
 *
 * Note: this uses a dedicated process-monotonic counter for the
 * inbox + sid so concurrent Request calls on the same handle
 * don't collide. Concurrent Request from different threads
 * against the same handle are still unsafe — the wire is
 * shared. Use separate handles per thread.
 */
#include <unistd.h>     /* getpid */

static int _amnats_req_counter = 0;

static inline code_string Amalgame_Messaging_NATS_Request(
        AmalgameNATS* n, code_string subject, code_string payload,
        i64 timeout_ms) {
    if (!n || n->fd < 0 || !subject) return (code_string) "";
    if (!payload) payload = (code_string) "";

    /* Generate unique inbox + sid for THIS request. */
    int req_id = ++_amnats_req_counter;
    int sid    = 1000000 + req_id;   /* high range, no collision with active_sid */
    char inbox[64];
    snprintf(inbox, sizeof(inbox), "_INBOX.%d.%d", (int) getpid(), req_id);

    /* SUB inbox sid */
    char sub_buf[128];
    int sub_len = snprintf(sub_buf, sizeof(sub_buf), "SUB %s %d\r\n", inbox, sid);
    if (_amnats_send_all(n->fd, (const unsigned char*) sub_buf, (size_t) sub_len) < 0) {
        n->last_error = _amnats_err_dup("Request: SUB inbox send failed");
        return (code_string) "";
    }

    /* PUB subject inbox payload */
    if (!_amnats_pub_internal(n, subject, inbox, payload)) {
        /* Pub failed; try to clean up sub. */
        char un[32];
        int un_len = snprintf(un, sizeof(un), "UNSUB %d\r\n", sid);
        (void) _amnats_send_all(n->fd, (const unsigned char*) un, (size_t) un_len);
        return (code_string) "";
    }

    /* Wait for reply. Reuse WaitMessage's drain loop with the
     * caller's timeout. The first MSG that lands ends up in
     * n->last_*; we don't filter by sid (the inbox is unique
     * enough that any unrelated server-side traffic is ignored
     * — non-MSG lines are drained transparently by WaitMessage). */
    code_bool got = Amalgame_Messaging_NATS_WaitMessage(n, timeout_ms);

    /* UNSUB regardless of outcome. */
    char un_buf[32];
    int un_len = snprintf(un_buf, sizeof(un_buf), "UNSUB %d\r\n", sid);
    (void) _amnats_send_all(n->fd, (const unsigned char*) un_buf, (size_t) un_len);

    if (!got) {
        /* Timeout — leave last_error empty (timeout isn't an error). */
        return (code_string) "";
    }

    /* Sanity check: returned MSG should have come on our inbox.
     * If not, treat as missed (shouldn't happen — UNSUB is best-
     * effort, server may still deliver a couple more, but
     * different sid means different sub). */
    if (n->last_subject && strcmp(n->last_subject, inbox) != 0) {
        /* Different subject — wasn't our reply. Caller still sees
         * it via last_*; we just return empty so they know
         * Request didn't get its expected response. */
        return (code_string) "";
    }
    return n->last_payload ? n->last_payload : (code_string) "";
}

#endif /* AMALGAME_MESSAGING_NATS_H */
