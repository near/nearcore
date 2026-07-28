# Playbook: assessing a PR's protocol impact with the model

How to use this model to find the effects of a pull request *beyond the immediate code
change* — the second-order impact a diff-only review misses.

## Mental model

Same as the [NEP review](nep-review.md): specs document **current behavior** with
`file:line` citations and `Interactions` sections. A PR is a concrete delta to the
code. The model's job here is to answer "what *else* does this touch?" — the
components downstream of the changed files, via the Interactions graph.

The difference from a NEP review: a PR is already code, so you map **changed files →
components** instead of reading a proposal. The specs' headers (the "Primary
crates/files" line) are the index for that mapping.

## Setup

- Run Claude Code **from the repo root**.
- Have the PR in reach: the branch checked out, or the diff (`git diff main...HEAD`),
  or the PR URL/number for the `gh` CLI.

## Prompt 1 — map the diff to components

> Get the PR diff (`git diff <base>...<head>` or `gh pr diff <n>`). Read
> `protocol-model/INDEX.md`. For each changed file, map it to the owning component(s)
> using the "Primary crates/files" header line in each `protocol-model/spec/*.md`.
> Output a table: changed file → component(s) → what the change appears to do. Note any
> changed file that maps to NO component (possible model gap). Don't analyze impact
> yet.

## Prompt 2 — trace impact per affected component

Fan out one subagent per directly-changed component.

> For each component the diff changes, spawn a subagent that reads its
> `protocol-model/spec/<component>.md`. Compare the diff against the spec's Behavior
> and Invariants: (1) does the change alter a documented Behavior step, and how; (2)
> does it preserve every Invariant — call out any it weakens or removes; (3) follow the
> spec's **Interactions** section to list downstream components that could be affected
> even though the PR does not touch their files; (4) does the change affect anything the
> spec marks as an **Open question**; (5) protocol-version / serialization / state-format
> implications. Cite spec sections and the relevant diff hunks. Flag uncertainty.

## Prompt 3 — synthesize + adversarially check

> Consolidate into a PR impact assessment: what the PR changes (by component),
> second-order effects on components it does NOT touch (via Interactions), invariants at
> risk, protocol-version/compat/state-migration concerns, and a ranked list of things a
> reviewer should specifically check or test. Then adversarially challenge each
> downstream-effect claim: is it supported by a spec Interaction + code, or speculative?
> Demote speculative or Open-question-dependent claims to "needs verification."

## Tips

- The high-value output is the **downstream / second-order** list — effects on
  components the PR doesn't visibly touch. That is precisely what reading only the diff
  misses and what the Interactions graph surfaces.
- A changed file that maps to no component, or a change that contradicts its spec, is a
  signal the model is stale for that area — note it for regeneration
  (`../REGENERATION.md`).
- For PRs that add or flip a `ProtocolFeature`, cross-check the activation story in
  [protocol-versioning](../spec/protocol-versioning.md).
