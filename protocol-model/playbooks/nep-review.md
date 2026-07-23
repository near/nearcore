# Playbook: reviewing a NEP with the protocol model

How to use this model to assess the protocol impact of a NEAR Enhancement Proposal.

## Mental model

Each spec under `../spec/` documents **current behavior at the pinned protocol
version** (see `../README.md`). A NEP proposes a **delta**. A good review is
therefore: *the delta + what it ripples into*. Two features of the specs make this
work:

- **`Interactions` sections** trace ripples — what each component consumes/produces
  and which siblings it touches.
- **`file:line` citations** let you drop into the real code wherever the NEP touches
  something a spec only summarizes.

The model is **descriptive, not exhaustive** — every spec has an `Open questions`
section recording what could not be derived from code. A review that lands on one of
those must say "verify this," not assert.

## Setup

- Run Claude Code **from the repo root** so relative links between specs resolve and
  Claude can verify claims against the actual source the specs cite.
- Get the NEP in reach: save to a file and pass the path, give Claude the URL
  (`https://github.com/near/NEPs/blob/master/neps/nep-XXXX.md`) to WebFetch, or paste
  it inline.
- Do **not** pre-load all 16 specs. Scope first (Prompt 1), then go deep on the few
  that matter. The whole model is small enough (~40k tokens) to load at once for a
  tiny NEP, but scoping yields a sharper, cheaper review.

## Prompt 1 — scope

> Read `protocol-model/INDEX.md` and `protocol-model/GLOSSARY.md`. Here is the NEP:
> `<path / URL / pasted>`. Identify which of the 16 components it touches, ranked by
> impact — for each give the component, its spec file, and one sentence on why. Also
> list the `ProtocolFeature`(s) it would add or require (check
> `core/primitives-core/src/version.rs`). Output a short table. Don't analyze yet.

## Prompt 2 — analyze each touched component

Fan out one subagent per touched component so each reads deeply in its own context.

> For each affected component, spawn a subagent that reads
> `protocol-model/spec/<component>.md` and `protocol-model/CONVENTIONS.md`. Treating
> the spec as CURRENT behavior at the pinned version, have it produce: (1) the precise
> behavioral **delta** the NEP introduces, mapped to the spec's numbered Behavior
> steps; (2) which **Invariants** are preserved / weakened / newly required; (3)
> **ripple effects** to other components via this spec's Interactions section; (4)
> **backward-compat / migration** concerns — state layout, wire format, versioning,
> activation; (5) anything the NEP affects that the spec lists under **Open questions**
> or doesn't cover. Cite spec sections, and where the NEP changes something the spec
> only summarizes, drill into the cited `file:line` in the real code to confirm. Flag
> uncertainty — do not guess.

## Prompt 3 — synthesize + adversarially check

> Consolidate the per-component analyses into one NEP impact review: executive
> summary, affected components with their deltas, cross-component / second-order
> effects, protocol-version & upgrade/activation concerns, backward-compat & migration
> risks, security/economic considerations, and a ranked list of open risks to verify
> against code or with the owning engineers. Then do one adversarial pass: challenge
> each claim — is it grounded in a spec section or code citation, or is it inferred?
> Demote anything that rests on a spec **Open question** to "needs verification."

## Tips

- The adversarial pass is not optional — it is what keeps the review honest given the
  model's known blind spots. Cite-or-demote.
- If the NEP introduces a brand-new `ProtocolFeature`, check `version.rs` for naming
  conventions and the activation mechanism in
  [protocol-versioning](../spec/protocol-versioning.md).
- If a touched component's spec looks stale relative to the code you read, that is a
  finding too — note it so the model can be regenerated (`../REGENERATION.md`).
