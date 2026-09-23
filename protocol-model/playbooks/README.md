# Playbooks

Copy-paste workflows for putting the protocol model to work. Each is a set of staged
prompts (scope → analyze → synthesize + adversarial check) designed to be run with
Claude Code from the repo root.

- [nep-review.md](nep-review.md) — assess the protocol impact of a NEAR Enhancement
  Proposal (NEP).
- [pr-review.md](pr-review.md) — assess a pull request's effects *beyond* the immediate
  diff, via the components' Interactions graph.
- [release-review.md](release-review.md) — assess everything that changed between the
  model's pinned baseline and a target commit; doubles as a regeneration trigger.

Both rest on the same idea: the specs under `../spec/` describe **current** behavior
with `file:line` citations; the thing under review is a **delta**; the review is the
delta plus what it ripples into. The final adversarial pass demotes any claim that
rests on a spec's `Open questions` to "needs verification."

Related: `../REGENERATION.md` rebuilds the model for a new release.
