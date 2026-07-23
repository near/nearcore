# NEAR protocol model (implementation-derived)

> **Protocol version: 86** (stable; min supported 83, nightly 155)
> **Release: 2.13.0** · **Derived from commit:** `499283a5e3a6f8ea52bc068c28e3a7bebb1e38c0` (2026-07-09)
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

Complete for the 2.13.0 release (protocol version 86):
- All scaffolding (this file, `INDEX.md`, `CONVENTIONS.md`, `REGENERATION.md`,
  `GLOSSARY.md`) and all 16 generation plans under `plans/`.
- All 16 component specs under `spec/` written and cite-verified (every non-trivial
  behavioral claim cited to `file:line`/symbol; an independent verification pass
  re-read the citations and folded corrections in). See `INDEX.md` for the list.

To refresh for a new release, follow [`REGENERATION.md`](REGENERATION.md).
