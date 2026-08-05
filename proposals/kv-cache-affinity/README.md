# Proposal: Session Affinity for KV-Cache Reuse

## Summary

Inference requests are routed to a pseudo-random node on every call. That is correct for fairness and validation, but it throws away the most valuable piece of serving state: the **KV / prefix cache**. A serving node (vLLM) keeps, per GPU, the attention key/value tensors for prefixes it has already processed. When a client's follow-up request in the same conversation lands on a *different* node, that node must recompute prefill for the whole shared prefix (system prompt + chat history) from cold — which, for multi-turn chat, is the dominant latency.

This proposal adds **bounded, opt-in session affinity**: a client that tags a conversation with the OpenAI-standard `prompt_cache_key` request field (fallback `user`) has its follow-up requests steered back to the same serving GPU, so the warm cache is reused. It is a pure scheduling hint — it does **not** change the nonce→host binding, the signed diff chain, payment, or validation — and it is bounded so no node can be pinned to a client indefinitely.

Non-goals: this does not trust, verify, or reward any node-reported cache hit; it does not add a global cache index or cross-node cache sharing; it does not change reward accounting.

## Problem

Routing is two hops, and the cache lives at the far end of the second one:

1. **Gateway → participant.** The devshard gateway (`devshardctl`) binds each inference nonce to a participant deterministically, `hostIdx = nonce % len(group)`, cycling round-robin over the escrow's slot set. This spread is deliberate: it keeps validation sampling representative and stops any single host from steering all of a client's traffic to itself.
2. **Participant → mlnode.** A participant runs a *pool* of mlnodes (GPUs). Its broker (`decentralized-api`, `broker.getLeastBusyNode`) hands each inference to the lowest-`LockCount` node for the model, with no session awareness.

The KV cache is **per-mlnode (per-GPU)**. So even a client that happened to return to the same participant can still be dispatched to a different GPU and miss the warm cache. To reuse the cache end-to-end, the same session must land on the same participant (hop 1) *and* the same mlnode within it (hop 2).

## Proposed Solution

Give each client session an opaque key (the OpenAI-standard `prompt_cache_key`, fallback `user`, read from the request body — no gonka-specific header, so standard OpenAI clients need no adaptation) and steer that session back to its last-used serving node at both hops, for a **bounded** window, then re-randomise.

**Hop 1 — gateway → participant (`devshardctl`).** The gateway already tracks, per queued request, a *per-request exclude set* (participants it has already tried) that the session picker honors. Affinity reuses exactly that machinery, inverted: for a session's **primary** attempt, exclude every participant *except* its sticky one, so the picker matches the request only to a nonce that binds to that participant. Bounded by a hold timeout: if the sticky participant is briefly PoC-gated / throttled or the escrow is idle, the request falls back to natural round-robin routing. Speculative/redundant **secondary** attempts keep their normal other-host routing, so latency racing and cross-host validation are unaffected.

**Hop 2 — participant → mlnode (`decentralized-api` broker).** `AcquireMLNodeRequest` gains an optional `session_id`. When set, `getLeastBusyNode` prefers the mlnode this session last used, if it is not in the skip set and is currently available for the model; otherwise it falls through to the existing least-busy selection. `lockAvailableNode` records which node the session landed on.

**Bound (both hops).** A session sticks to a node for at most `MaxRequests` requests or `TTL` wall-clock, whichever comes first; then the binding is evicted and the session re-randomises. Bindings are held in a fixed-cap map (`MaxEntries`) with a lazy expiry+eviction sweep, so an untrusted stream of distinct session keys cannot grow memory without bound.

## How it works (request flow)

A single request, from the client body to vLLM and back. Both routing decisions are the same shape: no session key (or expired/over-bound) → today's behaviour; sticky key within its bound → steer back to the warm node.

```text
request  (body: prompt_cache_key = "conv-42", fallback "user")
 |
 +--> GATEWAY (devshardctl):  key = affinityKeyFromBody(body)
 |     |
 |     +--> no key ------------------------------> feature inert: behaves exactly as today
 |     |
 |     +--> HOP 1  gateway -> participant
 |           |
 |           +--> key unseen / expired ----------> nonce % len(group) ------> participant P (random)
 |           +--> key sticky, within bound ------> steer primary to same participant P
 |                  (bound = MaxRequests | TTL | HOLD; else fall back to random routing)
 |
 +--> session_id = key   (rides the JSON wire to P; NOT in the signed payload)
       |
       +--> PARTICIPANT P (decentralized-api):  HOP 2  participant -> mlnode  (broker.getLeastBusyNode)
             |
             +--> session_id unseen ------------> least-busy GPU (lowest LockCount) ----> node G
             +--> session_id sticky ------------> prefer same GPU G (if free for model) -> node G
                    |
                    +--> ENGINE, just before the vLLM call:
                    |      body = withCacheSalt(body, session_id)   # cache_salt = sha256(session_id)
                    |
                    +--> vLLM prefix cache on GPU G, keyed by (prompt tokens + cache_salt)
                           |
                           +--> first time on G ---------> cold prefill, warms the cache
                           +--> repeat same prefix ------> PREFIX HIT: skip prefill of the shared prefix
                                  |
                                  +--> response streams back the SAME path, raw byte passthrough
                                        usage.prompt_tokens_details.cached_tokens ------> client
```

Why the second turn is fast — same conversation, same warm GPU:

```text
req #1  conv-42   -> P, GPU G  -> cold prefill of the 10k-token prefix   (cached_tokens = 0)
req #2  conv-42   -> P, GPU G  -> PREFIX HIT on that 10k prefix          (cached_tokens ~= 10k, low latency)
req #3  (no key)  -> nonce%len -> random participant / GPU               (unchanged behaviour)
```

`cache_salt` is what makes reuse safe: two different clients with different keys get different salts, so their KV blocks never collide even on identical prompt text — the speed-up is scoped to one session, closing the cache-timing side-channel.

## Consensus & Security Analysis

**Fits consensus — nothing verified changes.** `nonce % len(group)`, the signed diff chain, and host catch-up are untouched; hosts verify the exact same nonce sequence they do today. Hop 1 only reorders *which queued request* the gateway matches to an upcoming nonce — the same class of gateway-local scheduling the picker already performs with its exclude set — and is implemented by reusing that machinery, so no new dispatch or verification path is introduced. Hop 2 is internal load-balancing on a participant's own GPUs, invisible to the chain. The `session_id` is never placed in a signed message and is never sent to another participant.

**No self-dealing / validation-evasion (hop 1).** The anti-abuse property of random routing is preserved by the bound: a session cannot be pinned to one host beyond `MaxRequests` / `TTL`, so validation sampling keeps landing on other hosts probabilistically. Only the primary attempt is steered; secondaries still cross-check other hosts. And routing never affects payment — a host is paid only for the work it actually performs on funds the buyer supplied — so concentrating one's own bounded primaries earns nothing on top of the ~1/`groupSize` natural self-routing that already exists. No cache hit is trusted or rewarded.

**No new attack surface (hop 2).** These are one participant's own GPUs; steering a session among them cannot self-deal or dodge validation. The bound here exists purely so load can rebalance and a departed/rebalanced node is not chased.

**DoS / memory.** The affinity key (`prompt_cache_key`/`user`) is untrusted and uncapped in length, so the binding maps are size-capped with an eviction sweep. Creating a binding also costs the client a real, funded inference, so map growth is cost-bounded, not free.

## Implementation Status

- **Hop 1 (gateway, `devshardctl`): implemented.** `affinity.go` (bounded tracker), `InferenceParams.AffinityKey` from the OpenAI-standard `prompt_cache_key`/`user` request body fields (`proxy.go` via `affinityKeyFromBody`), primary steering in `redundancy.go` (`preparePrimaryWithAffinity`). OFF by default; enable via `DEVSHARD_AFFINITY_ENABLED`. Unit-tested (bounded stickiness, TTL, rotation eviction, map cap, disabled/empty no-ops).
- **Hop 2 (participant, `decentralized-api` broker): implemented, server side.** `session_id` on `AcquireMLNodeRequest`; broker preference + recording in `broker.go`; bounded tracker in `broker/session_affinity.go`; forwarded by the nodemanager gRPC server. Backward compatible — old clients send no `session_id` and get today's least-busy behaviour. Unit-tested.
- **cache_salt (security isolation) — implemented.** The devshardd engine injects vLLM's per-request `cache_salt = sha256(session id)` **host-side, immediately before the vLLM call** (`withCacheSalt`), NOT in the gateway's signed-prompt pipeline. `cache_salt` enters vLLM's KV-block hash so only same-salt requests reuse cached blocks (closes a cache-timing side-channel). It is output-invariant (cache namespacing only), so the committed/signed prompt and executor↔validator validation are unaffected (`VerifyPayload` ignores it; the validator replays with no salt). Different clients → different salt → no shared KV blocks. Unit-tested.
- **Session-id propagation — implemented.** The affinity key flows gateway → host → broker/engine: `SendOnly` sets it on `host.InferencePayload.SessionID`, carried over the JSON wire (`transport.PayloadJSON`) into `devshard.ExecuteRequest.SessionID`, threaded through the devshardd engine (`executeMLRequest` / `doWithLockedNode`) into `Client.Acquire` → `AcquireMLNodeRequest.session_id` (feeds hop-2 mlnode stickiness) and into the `cache_salt` injection. Not part of the signed payload (`VerifyPayload` ignores it). Feature end-to-end complete; inert at defaults (no key ⇒ no SessionID ⇒ no cache_salt, broker least-busy unchanged).

## Parameters

Gateway (hop 1), env:

| Var | Default | Meaning |
|---|---|---|
| `DEVSHARD_AFFINITY_ENABLED` | off | master switch |
| `DEVSHARD_AFFINITY_MAX_REQUESTS` | 10 | consecutive requests before re-randomise |
| `DEVSHARD_AFFINITY_TTL_MS` | 120000 | wall-clock lifetime of a binding |
| `DEVSHARD_AFFINITY_HOLD_MS` | 750 | wait for the sticky host before fallback |
| `DEVSHARD_AFFINITY_MAX_ENTRIES` | 50000 | binding-map size cap |

Participant broker (hop 2), env: `DAPI_MLNODE_AFFINITY_MAX_REQUESTS` (64), `DAPI_MLNODE_AFFINITY_TTL_MS` (600000), `DAPI_MLNODE_AFFINITY_MAX_ENTRIES` (50000).

## Files

- `devshard/cmd/devshardctl/affinity.go` (new) — gateway session tracker.
- `devshard/cmd/devshardctl/{proxy.go,redundancy.go}` — header read, primary steering.
- `devshard/user/session.go` — `InferenceParams.AffinityKey`.
- `common/nodemanager/nodemanager.proto` — `AcquireMLNodeRequest.session_id`.
- `decentralized-api/broker/session_affinity.go` (new) — broker mlnode tracker.
- `decentralized-api/broker/{broker.go,commands.go,node_lock.go}` — mlnode preference.
- `decentralized-api/nodemanager/server.go` — forward `session_id` to broker.
