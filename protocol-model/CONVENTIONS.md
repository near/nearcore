# Conventions

This file is the single source of truth for how every spec under
`protocol-model/spec/` is written and verified. Generation and verification
subagents are pointed here. If you change the template, update every spec on the
next regeneration.

## What this model is

A **descriptive** model of what nearcore *actually does*, derived from the
implementation at a pinned protocol version and git commit. It is **not** the
normative spec (that is the nomicon under `docs/`). When the model and the
nomicon disagree, the model describes reality and the nomicon should be fixed.

Audience: protocol engineers and LLMs. Optimize for being *correct, grounded, and
greppable*, not for prose polish.

## The citation rule

Every non-trivial behavioral claim must point at code. Use the form:

> `relative/path/from/repo/root.rs:LINE` — `function_or_type_name`

- The **function/type name is the stable anchor**; the line number is best-effort
  and is expected to drift between releases (regeneration refreshes it).
- Cite the *narrowest* construct that supports the claim (a function, a match arm,
  a constant) — not just the file.
- Prefer citing one authoritative location over many. If behavior is spread across
  several sites, cite each in the **Code anchors** table and reference them inline.
- Do not cite tests as the source of behavior; cite the production code. Tests may
  be cited *additionally* as evidence of an invariant ("asserted by …").

## Spec file template

Every `spec/*.md` MUST follow this skeleton, in this order:

```markdown
# <Component name>

> Protocol version: <N> (stable) · Derived from commit: <short-sha> · Generated: <date>
> Primary crates/files: `path/a`, `path/b`, …

## Role
One paragraph: what this component does and where it sits in the protocol. Link to
the components immediately upstream/downstream.

## Key data structures
For each important type:
- **`TypeName`** — `path.rs:LINE` — what it represents, key fields, versioning
  (e.g. V0/V1 enum variants) and why.

## Behavior
The core of the spec. Step-by-step description of the algorithms/state machine.
Every step cites code. Use numbered steps or clearly ordered subsections. Describe
*actual* control flow, including the order operations happen in, because ordering
is frequently load-bearing in this protocol.

## Interactions
What this component consumes and produces, and which other components it touches.
Link to sibling specs with relative links: `[Runtime execution](runtime-execution.md)`.
Do not re-document another component's internals — link instead.

## Protocol-version-gated behavior
Each `ProtocolFeature` that changes this component: name, the version it activates,
and how behavior differs before/after. Cite `core/primitives-core/src/version.rs`.
If none, say "None known at version <N>."

## Invariants & failure modes
What must always hold. What happens when an input violates expectations (errors
returned, panics, slashing, dropped messages). Cite the enforcement site.

## Code anchors
A table mapping the key `file:LINE` → behavior, so a future reader (or verifier)
can re-derive the spec. Include every location cited above plus any others worth
knowing. Columns: `Location | Symbol | What happens here`.

## Open questions
Anything the generating agent could not determine from code alone, or that the
verification pass flagged as uncertain. Empty section is fine — keep the heading.
```

## Style rules

- Lowercase the first word of error/log message quotes to match the codebase
  convention; quote them verbatim otherwise.
- Use NEP numbers when a feature maps to one (e.g. "stateless validation (NEP-509)").
- Use relative links between specs and `docs/...` links to the nomicon / architecture
  docs where they are still accurate; note when a nomicon page is stale.
- Keep each spec self-contained enough to answer questions on its own, but push
  cross-component detail behind links. No duplication of another spec's Behavior.
- Tables and short numbered steps over long paragraphs.

## Verification rules (cite + verify)

A spec is not "done" until a verification pass has run. The verifier:

1. Re-reads each cited `file:LINE`/symbol and decides: **supported / refuted /
   uncertain**, with a one-line correction for anything not supported.
2. Checks the **Protocol-version-gated behavior** section against
   `core/primitives-core/src/version.rs` for missed features touching this component.
3. Checks that no **Behavior** claim lacks a citation.

Refuted/uncertain items are fixed in the spec where possible; whatever remains
unresolved is moved to **Open questions**. The verifier returns a structured
verdict (see `REGENERATION.md`).
