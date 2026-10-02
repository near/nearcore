---
name: feature-audit
description: Run a multi-agent security audit of one feature or subsystem, hunting only for seriously exploitable defects (funds loss, consensus break, targeted DoS), with every claim cross-checked by a different agent and then confirmed by a test. Use when asked to audit a feature for vulnerabilities before release, security-review a subsystem, or find exploitable holes in a body of new code. Not for reviewing a single PR: use pr-review for that.
argument-hint: [feature or subsystem]
allowed-tools: Agent, Workflow, Bash, Read, Grep, Glob, Write, Edit, TodoWrite
---

# Feature security audit

A pipeline for auditing a whole feature, not a diff. It fans out across the feature's surface, then
narrows: sweep, promote, triage, cross-check, confirm empirically, write up.

**This is expensive.** Cost scales with how many areas the feature splits into in phase 1 (roughly
one agent per area, plus phase 2's critic, 4-6 in phase 3, two reviewers per surviving claim in
phase 5, and 2-3 confirmation workers in phase 6). As a reference point, the run this was distilled
from split into 19 areas and used 47 agents and roughly 9.5M subagent tokens over three workflow
invocations, plus a research agent that spent about $8 of the user's cloud budget. Recompute the
estimate for the feature at hand from its own area count rather than quoting that figure verbatim,
confirm the user wants that before starting, and give them the estimate in that shape.

**Before writing a fix for anything in the runtime, the parameter set, or chunk production, read
`references/protocol-fix-traps.md`.** It covers safely fixing protocol-level findings; findings
confined to RPC, tooling, or other non-protocol surfaces don't need it.

## Before starting

1. **Define the target precisely.** Get the commit range or the set of PRs that introduced the
   feature. Build the evidence base once and reuse it: `git diff <base>..<head>` over the feature's
   commits into `evidence/feature.diff`, plus a `changed-files.txt`. Every agent gets pointed at
   these instead of rediscovering them.
2. **Decide the bar with the user, in writing.** "Serious and exploitable" needs examples: funds
   loss, supply inflation, consensus divergence, targeted denial of service, node panic,
   authorization bypass. Everything else is a note, not a finding.
3. **Check resources.** Disk, and whether long builds are safe here. Never propose `cargo clean`
   as a disk fix without saying it forces a full rebuild; prefer deleting caches that can be rebuilt.
4. **Set up the deliverable directory** outside the repo, so audit artifacts never get committed by
   accident: `<workspace>/<feature>-security-audit/{details,evidence,raw}`.

## Pipeline

Each phase's output is the next phase's input. Do not skip phases 2 and 3: in the source run, two of
the three confirmed findings came from phase 3 and would not have existed otherwise.

### Phase 1: Sweep

Split the feature into 12 to 20 areas by mechanism, not by file. The right split is specific to
what the feature actually does; typical mechanism areas for a nearcore protocol feature include
authorization, state transitions, fee and gas accounting, limits and validation, congestion and
queueing, RPC surface, tooling, protocol gating, and migration and replay, but a feature centered
on, say, identifier derivation, networking, or sync would add or drop areas accordingly. One agent
per area.

**Instruct finders to report observations, not findings.** This is the single most important
instruction in the phase. Ask for every anomaly traced to a `file:line`, with a one-line statement
of what looks wrong and what it would take to exploit, and explicitly tell them that a note they
cannot exploit alone is still wanted. In the source run 18 of 19 finders returned zero findings
while collectively producing 126 traced notes, several of which were serious once combined across an
area boundary. A bar applied at this stage suppresses the leads the audit exists to find.

Require of every agent: no speculation presented as fact, an explicit "unverified" label on anything
read rather than run, and no invented identifiers, file paths or parameter names.

### Phase 2: Completeness critic

One agent reads the diff and the area list and answers: what part of this feature did nobody look
at? Feed gaps back as extra phase 1 agents. Cheap, and it is the only defence against a fan-out
that silently missed a file.

### Phase 3: Promotion

Give 4 to 6 agents the *whole* note pool from phase 1, each with a distinct combination lens:

- fees and accounting versus limits and validation
- anything unbounded: counts, lengths, iterations, recursion depth
- work that is done before it is paid for, or paid for but not done
- state that crosses a shard, chunk or receipt boundary
- anything that differs between the transaction path and the contract path
- anything version-gated, and what happens at the boundary

Each asks: do any two of these notes compose into something exploitable? Output candidate claims in
a fixed schema (`id`, `title`, `severity`, `mechanism`, `code_refs`, `exploit_sketch`,
`what_would_disprove_it`).

### Phase 4: Triage

Normalize claims, merge duplicates, drop anything whose impact is below the bar even if true. Keep
the dropped list: it goes in the write-up so the same thing is not re-raised later.

### Phase 5: Cross-check

Every claim gets at least two reviewers, **none of whom may be its reporter**. Each returns
`REAL` / `NOT_REAL` / `UNCLEAR` with reasoning tied to code.

Three rules learned the hard way:

- **A claim with zero votes is `UNVERIFIED`, never `NOT_REAL`.** In the source run the orchestration
  script computed `not_real.length === votes.length`, which is `0 === 0` for a claim whose reviewers
  all died on an API session limit, silently marking two live claims refuted. Assert on vote count
  before interpreting verdicts.
- **Fewer than two live votes triggers a tiebreaker**, not a decision.
- **Never inherit a verdict from an agent that died.** Re-review from scratch.

### Phase 6: Empirical confirmation

Two or three long-lived worker agents pulling from a shared claim queue via a cursor, rather than
one agent per claim. This is what keeps a large claim set inside a compute budget.

Every worker writes a real test that asserts the **correct** behaviour, so it fails on the unfixed
code and passes once fixed. A claim that cannot be turned into a failing test is not confirmed;
label it as a code read and say so in the write-up.

Require each worker to report the exact command, the real output, and which specific link in the
claim's chain each number proves. Reporters' arithmetic is frequently wrong in both directions: in
the source run one reporter was 6x off on hashing throughput while its conclusion was too
conservative by 2x. Re-derive every number from parameters you have read yourself.

### Phase 7: Write-up

- `SUMMARY.md`: verdict, a findings table, and one section per finding capped at 200 words, each
  linking to its detail doc. Include a section for claims that did **not** survive testing, and one
  for what was examined and found sound.
- `details/<ID>.md` per finding: claim, what was tested, exact commands, real output, what each
  number proves, root cause with `file:line`, fix, residual uncertainty.
- `evidence/` and `raw/`: the diff, the note pool, machine-readable phase outputs, agent notes.
- Record every figure's provenance. A number without a parameter file and line behind it will be
  wrong eventually, and will be quoted anyway.

## Hard rules

**On measurement**

- **Test the worst input the code will accept, not a representative one.** This is the lesson that
  cost the most. A per-action limit passed a single-action test and was trivially bypassed by 65
  byte-identical actions in one receipt, restoring the full attack. Every test in the audit had
  exercised one action, so the finding, the suggested fix and the regression guard shared one blind
  spot. Construct the extreme the validator still permits, and assert the bound only for input that
  validation actually accepts.
- **Find the constraint that actually binds.** Two findings were mis-modelled until this was asked
  explicitly. One looked gas-bound but was bytes-bound, because a receipt has to exceed a bandwidth
  grant before it is buffered at all. Costs, minimum viable payloads and fixes all change.
- **Never assert on wall-clock elapsed time.** Assert on counts, states or reasons. An elapsed-time
  bound flakes on a loaded worker even when the logic is right. Calibrate any timing-dependent load
  against a measured warmup so the test does not depend on absolute machine speed.
- **Prove the guard is a guard.** Disable the fix, run the test, confirm it fails, restore. A test
  that passes both before and after is worthless and looks valuable.
- **State the residual, quantified.** "Fixed" usually means "made much harder". Say by how much: one
  receipt going from 191% to 11.5% of a cap is a real fix and still not zero.

**On cost models**

- Separate what an attacker **burns** from what they **reserve**. Reserved-and-refunded gas is
  nearly free, and that asymmetry made one finding 13x cheaper than the other.
- Check for self-damping: does sustaining the attack raise the gas price, or fill a queue that
  throttles it? An attack that damps itself is a different severity.
- State the assumption behind any per-day figure. Block time is the usual hidden variable.

**On agents**

- Give every subagent a spend and scope ceiling. Say what credentials and paid services it may not
  touch. A research agent in the source run reasonably used the user's cloud project and ran up a
  bill because the brief did not bound it.
- Expect agents to spawn their own agents. Count on fan-out when estimating cost.
- Never read a subagent's transcript file; it will overflow your context. Wait for the result.
- Treat every agent result as a claim to verify. In the source run agent-reported figures were
  wrong on hashing throughput, on a transaction size limit, and on an occurrence count whose
  inflation was a harvesting artifact rather than a property of the chain.

**On disclosure**

- Distinguish unreleased code from live code before writing anything public. A fix to an unreleased
  feature can be described fully. A fix to code already live on mainnet must not carry the recipe:
  describe the missing bound and the mechanism, omit sizings, costs and impact framing.
- Never put private vulnerability identifiers in code, tests, commit messages or PR descriptions.
- If a finding turns out to be shared with already-shipped code, that changes both the disclosure
  handling and the fix scope. Check whether each finding is a regression of the new feature or a
  pre-existing defect the feature merely exposes a second route to. One of three findings in the
  source run was not a regression at all, and was the cheapest and broadest of them.

**On your own tooling**

- Never `git checkout <file>` to undo a temporary edit; it discards everything else in that file.
  This destroyed a finished test in the source run. Revert the specific edit instead.
- `pkill -f <pattern>` matches your own shell if the pattern appears in its command line.
