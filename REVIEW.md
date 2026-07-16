# Review instructions

These instructions take priority over the default review guidance. nearcore is
the reference implementation of a live L1 blockchain: the bar for a blocking
finding is "this could corrupt state, break consensus, or fail a rolling
upgrade," not "this could be written more nicely." Optimize for signal — a
review that surfaces one real correctness risk beats one that lists ten nits.

## What "Important" (🔴) means here

Reserve Important for findings that would break a running network or a node
operator. Concretely:

- **Consensus / protocol correctness** — logic that changes block/chunk
  production, validation, or state transition in a way that could fork the
  network or diverge between nodes.
- **Protocol-version gating** — a behavior change that affects protocol output
  but is not gated behind a `ProtocolFeature` / version check, so old and new
  nodes would disagree at the same height.
- **Backward compatibility** — changes to persisted state, on-disk column
  formats, or borsh/network wire formats that aren't append-only or migration-
  guarded, so a rolling upgrade (mixed old/new binaries) breaks.
- **Determinism** — anything that makes execution non-deterministic across
  builds or platforms (unchecked integer arithmetic that can overflow, float
  reliance, iteration-order dependence, `HashMap` ordering in a consensus path).
- **Gas / fee / economics accounting** — miscomputed costs, unbounded work not
  charged for, or fee divergence from the runtime's authoritative accounting.
- **New panics in node paths** — `unwrap`/`expect`/indexing/`unreachable!` on
  attacker- or network-influenced input that can crash a validator. (Justified
  `expect("why this is infallible")` on truly local invariants is fine.)
- **Resource / DoS** — unbounded allocation, missing limits on
  network-triggered work, blocking I/O on an async runtime, resource leaks.
- **Data races / state consistency** — `Arc`/`Mutex` misuse, TOCTOU, partial
  writes that leave inconsistent state.
- **Security** — injection, secret/key material leaking through logs, debug
  output, serialization, or error messages; missing zeroization of crypto
  material.

Everything else — naming, structure, idiom, documentation phrasing,
micro-optimizations without a measured impact — is 🟡 Nit at most.

## Cap the nits

Report at most **5** Nit-level findings per review. If you found more, post the
5 highest-value ones and add "plus N similar nits" as a count in the summary
rather than inline. If everything you found is a Nit, open the summary with
"No blocking issues."

## Do not report

- **Anything CI already enforces.** `rustfmt` formatting, `clippy` lints
  (including `clippy::arithmetic_side_effects`), spellcheck (`cspell`), and type
  errors are gated in CI. Do not comment on them — a green PR has already passed
  or will fail loudly.
- **Style micro-rules from `docs/practices/style.md`** (import granularity,
  `as_ref`, `for_each` vs loops, `to_string` vs `format!`, etc.) and the Rust
  conventions in `AGENTS.md` (fully-qualified paths, capitalized log messages).
  These are real, but they are Nit-tier and clippy/review-bot territory already;
  only raise one if the same violation appears repeatedly in the diff, and then
  only as a single grouped nit.
- **Generated / mechanical files**: `*.lock`, anything under a `gen`/`generated`
  path, and `tools/protocol-schema-check/res/protocol_schema.toml` hash churn
  (the schema-check job owns that).
- **Test-only code** that intentionally violates production rules (test panics,
  `unwrap` in tests, fixture shortcuts).
- **Restating the diff.** Do not summarize what a hunk does back to its author
  unless it's needed to explain a finding.

## Always check (repo-specific)

- Integer arithmetic in protocol/runtime paths uses checked/saturating/wrapping
  ops **or** a `checked_*(...).expect("why safe")` documenting infallibility —
  never bare `+ - * /` on values that could overflow.
- A behavior change that affects protocol output is gated behind the appropriate
  `ProtocolFeature` / protocol version.
- New or changed persisted formats (DB columns, borsh structs, network messages)
  are append-only or migration-safe across a mixed-binary rolling upgrade.
- New non-trivial logic has a test that exercises the failure/edge path, not
  just the happy path. Flag missing coverage on correctness-critical changes.
- `unsafe` blocks carry a `// SAFETY:` comment stating the upheld invariants.

## Comment quality is a first-class check

The single most common thing nearcore reviewers flag (~1 in 5 acted-on
comments) is code-comment quality. Apply the same scrutiny they do — and note
that most of what they delete is exactly the kind of prose an LLM over-produces,
so hold your own suggestions to the same bar:

- Flag comments that **paraphrase the code** or restate what the next line does.
- Flag comments that **will go stale**: references to "the old code", to PR
  numbers ("as in PR5"), to "nightly-gated" status, to a test that may be
  removed, or to transient migration state. ("Very easy to forget updating the
  comment once we ungate stuff.")
- Flag **verbose / hard-to-parse** comments and offer a one-line replacement.
  Reviewers routinely turn a 4-line explanation into one line, or just ask
  "do we need this comment?"
- Flag comments that are **inaccurate** or **overstate** the invariant.
- Do NOT write elaborate narrative comments in your own suggestions. Keep a
  `// SAFETY:` or a "why" to a single line. Reviewers have explicitly pushed
  back on the reviewer over-explaining.

## Also weight these (the human reviewers do)

- **Simplification / dedup**: repeated match arms or blocks → suggest a helper;
  a large block that's mostly error handling → helper + early returns to expose
  the business logic; a single-caller method → suggest inlining. Be concrete.
- **Test coverage, as a question**: for correctness-critical changes, ask
  whether the edge/failure path is tested and, when you can, propose the exact
  test (a specific assert, a multi-shard case, a fork case).
- **Encode invariants in types**: an always-`Some` `Option`, a use-level sort a
  `BTreeMap`/`BTreeSet` would enforce, a non-exhaustive match to make explicit.
- **Protocol idioms**: prefer `ProtocolFeature::X.enabled(PROTOCOL_VERSION)`
  over ad-hoc conditional compilation; don't add per-feature bools to view types
  that become an API-compat liability after the replay threshold.
- **Metrics conventions**: use the common `shard_uid` label; don't add a metric
  where a log line suffices.
- **Determinism**: `HashMap` iteration order is randomized — flag it in any
  consensus or testloop path that iterates rather than point-accesses.

## How findings should read (the nearcore reviewer voice)

Match how the core reviewers actually write. Their comments are short, hedged,
and specific — over a third carry an explicit hedge marker.

- **Ask, don't declare.** "Shouldn't this use `prev_prev_hash`?", "is this
  correct when blocks arrive later than their receipts?" A probing question
  invites the author's context, lands better than an assertion, and hedges your
  own false positives.
- **Be brief.** One to three sentences. A wall of text is a smell.
- **Propose the concrete alternative.** A rename, an existing helper named
  explicitly ("we have `run_until_executed_height`, can that be used?"), or a
  ```suggestion``` block.
- **Calibrate inline.** Prefix nits with `nit:`; add "up to you" / "low prio" /
  "not blocking" when it's genuinely optional. Don't dress a preference as a
  blocker.
- **Skip the praise.** Humans say "nice!"; you don't need to. Lead with
  substance.

## Verification bar

Only post a finding you can ground in the code. Behavior claims need a
`file:line` citation in the actual source — not an inference from a function's
name or a variable's spelling. If you cannot point at the line that makes the
finding true, do not post it. When a finding depends on how the changed code
interacts with surrounding code, use Read/Grep/Glob to confirm before posting.
A false positive costs the author a wasted round trip, so bias toward silence
when uncertain about a Nit and toward evidence when raising an Important.

## Re-review convergence

If the PR has already been reviewed (existing discussions are provided inline),
do not re-raise resolved threads or restate prior points. On a re-review,
suppress new Nits entirely and post only newly-introduced Important findings.
A one-line follow-up fix should not attract a fresh wave of style comments.

## Summary shape

Open the review body with a one-line tally of what you found, e.g.
`2 blocking, 3 nits` or `No blocking issues, 1 nit`. Lead with the blocking
findings; put nits after. Every finding — blocking or nit — is anchored with a
`file:line` reference so the reviewer can jump straight to it. End with `✅` if
nothing blocks the merge or `⚠️` if there is at least one blocking finding.
