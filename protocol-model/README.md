# NEAR protocol model (implementation-derived)

> **Protocol version: 88** (stable; min supported 84, nightly 157)
> **Release: 2.14.0** · **Derived from commit:** `bc4e9e8cfb03d9e82f027883055624e18576993e` (2026-10-26)
> **Status:** complete — scaffolding + all 16 component specs written and cite-verified. See "Status" below.

This is a **descriptive behavioral model** of the NEAR protocol as actually
implemented by nearcore at the pinned version above. It is built *from the code*,
with every non-trivial claim cited to a `file:line`/symbol, and each spec is
cross-checked against the implementation before being considered done.

It is **not** the normative specification — that is the [nomicon](https://nomicon.io)
(source under [`docs/`](../docs/)). The nomicon says what implementations *should*
do; this model says what nearcore *does*. Where they disagree, this model describes
reality and the nomicon should be corrected. One intended use of this model is
exactly that: fixing stale docs.

## What it's for

- **Ask an LLM protocol questions**: feed [`INDEX.md`](INDEX.md) + the relevant
  `spec/*.md` files and ask.
- **Read it directly**: start at [`INDEX.md`](INDEX.md).
- **Fix outdated docs**: point an LLM at this model + a doc source and ask it to
  reconcile.
- **Review a NEP**: point an LLM at the NEP + this model for a grounded impact review.
- **Assess a PR**: point an LLM at the diff + this model to find effects beyond the
  immediate code.

## Layout

| Path | What |
|------|------|
| [`INDEX.md`](INDEX.md) | Component map + cross-link graph. The entry point. |
| [`CONVENTIONS.md`](CONVENTIONS.md) | Spec template + citation/verification rules. |
| [`REGENERATION.md`](REGENERATION.md) | Reusable task to rebuild the model for a new release. |
| [`GLOSSARY.md`](GLOSSARY.md) | Shared protocol terms. |
| [`playbooks/`](playbooks/) | Staged prompt workflows: [NEP review](playbooks/nep-review.md), [PR review](playbooks/pr-review.md), [release review](playbooks/release-review.md). |
| [`plans/`](plans/) | One reusable generation plan per component. |
| [`spec/`](spec/) | The specification itself. |

## How it's kept current

The model is pinned to a protocol version and commit. To regenerate for a new
release, follow [`REGENERATION.md`](REGENERATION.md): it re-runs the per-component
plans in [`plans/`](plans/) against the new code and re-verifies every claim.

## Status

Complete for the 2.14.0 release (protocol version 88):
- All scaffolding (this file, `INDEX.md`, `CONVENTIONS.md`, `REGENERATION.md`,
  `GLOSSARY.md`) and all 16 generation plans under `plans/`.
- All 16 component specs under `spec/` updated against the new code and cite-verified
  (every non-trivial behavioral claim cited to `file:line`/symbol; an independent
  verification pass re-read every citation and folded corrections in). See `INDEX.md`.

Re-pinned from the 2.14.0-rc.1 edition (version 87, commit `233252e`) to the final
2.14.0 release. The protocol moved during the release-candidate cycle, so the delta is
small in files but material in rules:

- `STABLE_PROTOCOL_VERSION` 87 → **88**; min supported 84 and nightly 157 unchanged.
- Four new features — `RejectUniversalAccountTransfers` and `RejectBlockHeaderV7BeforeSpice`
  at 87, `RefundRepeatedUniversalStateInitDeposit` and `CanonicalUniversalStateInit` at 88.
- `UniversalAccounts` **moved from 87 to 88**, giving a deliberate two-step activation.
- `ReceiptPromiseInputSizeLimit` and `FixMlDsaCostCharging` were **de-stabilized** back to
  nightly (157) before release, so neither is active on the stable path.
- Ungated: `BlockHeaderV8` (reverting an rc-only change to `BlockHeaderV7`), state-sync
  block-body validation, contract-code responses grouped by size, an early-kickout
  cache-eviction fix, and cold-store RocksDB tuning for archival nodes.

The earlier 2.13.0 → 2.14.0-rc.1 round rebuilt the model wholesale: the NearVM engine
crates were deleted (wasmtime is the only backend), centralized external-storage state
sync was removed, and the networking layer gained a release of memory, rate and size
limits.

To refresh for a new release, follow [`REGENERATION.md`](REGENERATION.md).
