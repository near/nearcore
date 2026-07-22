# Playbook: reviewing a release with the model

How to use this model to assess the impact of everything that changed between the
**last known stable release** (the commit this model is pinned to) and a **target
commit** you supply — a candidate release, release branch, or `master`.

## Mental model

This is the [PR review](pr-review.md) at release scale. The model under `../spec/`
describes behavior at its **pinned baseline** (`BASE` — the commit stamped in
`../README.md`). You provide a **target** (`TARGET`) to compare against. The review is
the delta `BASE...TARGET` plus what it ripples into.

Two kinds of change matter **equally** — do not let the first crowd out the second:

1. **Protocol-gated changes** — new/changed `ProtocolFeature`s and any
   `STABLE`/`NIGHTLY`/`MIN_SUPPORTED` version bumps. These change consensus rules, and
   only at activation. The feature delta is a useful *lens* (it points at which
   components can have consensus-visible changes) — but it is **not** the whole review.

2. **Ungated behavioral changes** — changes to `neard` behavior that take effect **on
   upgrade**, independent of protocol version, because they are gated by config, by the
   handshake-negotiated network version, or by nothing at all. These are frequently the
   ones with surprising effects and are easy to miss because no `ProtocolFeature`
   flags them. They include, at least:
   - **Network / node-to-node**: wire format & message types, handshake, peer
     discovery / scoring / banning, routing, tiers, rate limits.
   - **Sync & catchup**: strategy, ordering, timeouts, peer-vs-external/cloud sources,
     state-part serving.
   - **Storage**: DB column layout, on-disk formats, flat storage, GC, snapshots,
     hot/cold split.
   - **Operational**: config defaults and validation, tracked-shards / archival
     behavior, RPC surface and semantics, resource limits, and performance changes that
     shift block/chunk timing enough to affect liveness.

A release review must cover **both** axes. A change with no `ProtocolFeature` is *not*
presumed safe — in particular, ungated changes must stay **interoperable across a
mixed-version network** during a rolling upgrade (old and new nodes coexisting), which
protocol activation does not protect.

The model is descriptive, not exhaustive — every spec has an `Open questions` section,
and some changed areas (e.g. RPC, tooling, pure operational concerns) may map to **no**
component at all. Treat both as "verify manually," not "no impact."

## Setup

- Run Claude Code **from the repo root** so relative links resolve and Claude can read
  both the model and the actual source at `TARGET`.
- `BASE` = the commit stamped in `../README.md` (also in every spec header). Read it —
  don't hardcode — so this playbook stays correct after each regeneration.
- `TARGET` = the commit / tag / branch under review. The diff is `BASE...TARGET`.
- The diff is large. Scan broadly across protocol-relevant crates
  (`chain/ core/ runtime/ nearcore/ neard/`) and don't pre-filter to consensus code.

## Prompt 1 — scope the full delta (both axes)

> Read `protocol-model/README.md` and take its pinned commit as `BASE`. The target to
> review is `TARGET = <commit / tag / branch>`.
>
> Produce the scope along **two axes**:
>
> **(A) Protocol-gated delta.** `git diff BASE...TARGET -- core/primitives-core/src/version.rs`
> — list every new, changed, or removed `ProtocolFeature` and any change to
> `STABLE_PROTOCOL_VERSION` / `NIGHTLY_PROTOCOL_VERSION` / `MIN_SUPPORTED_PROTOCOL_VERSION`.
>
> **(B) Behavioral delta (all changes, gated or not).** `git diff --stat BASE...TARGET`
> over `chain/ core/ runtime/ nearcore/ neard/`. Map every changed file to component(s)
> via the "Primary crates/files" header in each `protocol-model/spec/*.md`. For each
> change, classify it as: protocol-gated / config-gated / network-version-gated /
> ungated-on-upgrade, and note whether it looks behavioral or a pure refactor (but do
> **not** discard "refactors" — a refactor that changes timing, ordering, error
> handling, or wire/DB bytes is behavioral).
>
> Output three tables: (a) protocol-feature / version delta; (b) changed-file groups →
> component(s) → gating class → one-line summary; (c) changed files that map to **no**
> component (RPC, tooling, ops, etc.) — these need manual attention. Don't analyze impact
> yet.

## Prompt 2 — analyze each affected component

Fan out one subagent per affected component.

> For each affected component, spawn a subagent that reads
> `protocol-model/spec/<component>.md` (which describes `BASE` behavior) and inspects
> `git diff BASE...TARGET` for that component's files. Produce, **regardless of whether
> the change is `ProtocolFeature`-gated**: (1) the behavioral **delta** vs the spec's
> numbered Behavior steps; (2) **Invariants** preserved / weakened / newly required; (3)
> **ripple effects** to other components via this spec's Interactions section; (4) how
> the change is **gated** (protocol version, config, handshake/network version, or
> ungated) and when it takes effect; (5) **mixed-version interoperability** — can a
> `BASE` node and a `TARGET` node coexist during a rolling upgrade (wire compatibility,
> DB migration, sync), and what breaks if not; (6) **backward-compat / migration** —
> state layout, on-disk format, serialization, config defaults; (7) anything touching
> the spec's **Open questions**. Cite spec sections and diff hunks / `file:line` at
> `TARGET`. Flag uncertainty — do not guess.
>
> For the un-modeled changed files from Prompt 1 table (c), do a lightweight pass: what
> behavior changed and who is affected (node operators, RPC consumers, archival,
> tooling), even though no spec covers it.

## Prompt 3 — synthesize + adversarial check + staleness list

> Consolidate into a release impact review with **two clearly separated parts**:
> **(I) Protocol/consensus changes** — version & new-feature summary, per-component
> consensus-rule deltas, activation & upgrade-voting concerns. **(II) Non-protocol /
> `neard` behavioral changes** — network & node-to-node, sync, storage, RPC, config and
> operational changes; their effect on a mixed-version network during rollout; and
> effects on node operators / archival / tooling. For both parts: cross-component &
> second-order effects, backward-compat & migration risks, security / economic /
> liveness considerations, and a ranked list of open risks to verify against code or
> with owning engineers.
>
> Then an adversarial pass: challenge each claim — grounded in a spec section + diff, or
> inferred? Demote anything resting on a spec **Open question**, or on a changed area
> with no spec coverage, to "needs verification."
>
> Finally, output the **regeneration worklist**: which component specs no longer match
> `TARGET` and should be rebuilt via `protocol-model/REGENERATION.md`, ordered by
> divergence — and note any behavioral areas with no component that the model should
> perhaps grow to cover.

## Tips

- **No `ProtocolFeature` ≠ no impact.** The most surprising release regressions
  historically come from ungated changes to networking, sync, and storage. Give axis
  (B) at least as much weight as axis (A).
- **Mixed-version network is the ungated-change analogue of protocol activation.**
  Protocol features are protected by epoch-boundary activation; an ungated wire/DB
  change is not — it must be backward compatible during the rolling upgrade. Always ask
  "can a `BASE` node and a `TARGET` node talk to / sync from / share a DB with each
  other?"
- Use axis (A) to triage *consensus* impact and axis (B)'s changed-file scan as the
  primary input — start from the feature delta, but never stop there.
- A release review is the natural moment to **regenerate the model**. Its staleness list
  is the worklist; after regenerating, the new model is pinned to `TARGET` and becomes
  the next review's `BASE`.
- For features nightly-only at `TARGET`, note them but mark them inactive on the stable
  path — the same distinction the specs draw in their Protocol-version-gated sections.
