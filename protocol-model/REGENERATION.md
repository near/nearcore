# Regeneration task — rebuild the protocol model for a new release

This is a standalone, reusable task. Running it recreates (or updates) the entire
`protocol-model/` for a new nearcore release. It assumes the `plans/` directory and
`CONVENTIONS.md` already exist; if generating from scratch, the plans must be
authored first (see "Bootstrapping" at the bottom).

## Inputs

- The nearcore checkout at the target release.
- The pinned **protocol version** and **git commit** for the release.

## Procedure

### 1. Re-pin the version

- Read `core/primitives-core/src/version.rs`: record `STABLE_PROTOCOL_VERSION`,
  `MIN_SUPPORTED_PROTOCOL_VERSION`, `NIGHTLY_PROTOCOL_VERSION`.
- Get the commit: `git rev-parse HEAD` and its date.
- Update the version/commit stamp in `README.md`, `INDEX.md`, and every `spec/*.md`
  header.

### 2. Generate / update each component spec

For each `plans/NN-*.md`, run **one generation subagent** with this brief:

> Read `protocol-model/CONVENTIONS.md` (template + citation rules) and
> `protocol-model/plans/NN-*.md` (your scope, code anchors, and questions). Read the
> listed code. Produce `protocol-model/spec/<name>.md` following the template
> exactly. Cite every non-trivial behavioral claim to `file:line` + symbol. Stay
> within your scope — link to sibling specs instead of re-documenting them. Output
> the full markdown file content.

Subagents for different components are independent and run in parallel.

**Update vs regenerate:** if a prior `spec/<name>.md` exists and the component's
code changed little (no new `ProtocolFeature`, no structural refactor of the cited
files), instruct the agent to *update* the existing spec (refresh line numbers, add
new gated behavior, fix drift) rather than rewrite it — cheaper and preserves prose.
Regenerate from scratch when the component changed substantially. Decide per
component by diffing the plan's listed files against the previous pinned commit.

### 3. Verify each spec (cite + verify)

For each freshly written/updated spec, run **one verification subagent**:

> Read `protocol-model/CONVENTIONS.md` (verification rules) and the spec at
> `protocol-model/spec/<name>.md`. For every code citation, re-read the cited
> `file:line`/symbol and judge whether it supports the claim. Also check the
> *Protocol-version-gated behavior* section against
> `core/primitives-core/src/version.rs` for missed features, and confirm no
> *Behavior* claim is uncited. Return JSON:
> `{ "claims": [{ "claim", "location", "verdict": "supported|refuted|uncertain",
> "correction" }], "missing_features": [...], "uncited_claims": [...] }`.

Fold corrections back into the spec. Move anything still unresolved into the spec's
**Open questions** section. A spec is done only after this pass.

### 4. Rebuild navigation

- Update `INDEX.md`: flip status markers, and re-derive the cross-link graph if
  components were added/removed/merged.
- Update `GLOSSARY.md` with any new shared terms surfaced by the specs.

### 5. Record the diff

Summarize what changed versus the previous pinned version (new/removed components,
new `ProtocolFeature`s incorporated, specs that were rewritten vs updated). Put this
in the regeneration's final report (not committed into a spec).

## Suggested orchestration

This is a natural fit for a fan-out workflow: pipeline each component through
`generate → verify`, with the two stages running per-item (a component can be
verifying while another is still generating). Components are independent, so there is
no barrier between them. ~16 components × 2 stages. Keep `CONVENTIONS.md` and the
relevant `plans/NN-*.md` in each subagent's prompt.

## Bootstrapping (first-time / new component)

To add a component or build the plans from nothing, author a `plans/NN-*.md` first
using the structure shared by the existing plans:

- **Scope** (what's covered) and **Out of scope** (what belongs to a sibling).
- **Code to read** — ordered list of crates/files/modules.
- **Questions the spec must answer.**
- **Cross-component edges** to link.
- **Relevant `ProtocolFeature`s** to check.
- Pointer to `CONVENTIONS.md`.

Then run steps 2–4 for it.
