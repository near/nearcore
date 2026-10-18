---
name: cleanup-old-protocol-version
description: Bump MIN_SUPPORTED_PROTOCOL_VERSION by one and perform the corresponding old-protocol-version cleanups — remove deprecated ProtocolFeature variants, inline their enabled() checks, and clean up the matching runtime-parameter diff file. Use when the user wants to deprecate an old protocol version per issue #16564.
allowed-tools: Read, Grep, Glob, Bash, Edit, Write, AskUserQuestion
---

Deprecate the next old protocol version per
[issue #16564](https://github.com/near/nearcore/issues/16564). Historical
reference PRs: #14729 (inline a wasm_config param), #14756 (delete the
corresponding `.yaml`), #16454 (inline always-true flags from
`parameters.yaml`).

The skill takes no arguments and produces **a single PR** that bundles
every cleanup for one `MIN_SUPPORTED_PROTOCOL_VERSION` increment. It:

1. Reads `MIN_SUPPORTED_PROTOCOL_VERSION` from `core/primitives-core/src/version.rs`.
2. Sets `new_min = old_min + 1`.
3. Removes every `ProtocolFeature` variant with `protocol_version() <= new_min`
   and inlines each one's `.enabled(...)` call sites.
4. For each yaml under `core/parameters/res/runtime_configs/` whose version
   is `<= new_min` (typically just `{new_min}.yaml`): inlines the non-overridden
   parameters, deletes the file, and removes its `CONFIG_DIFFS` entry.
5. Inlines any `parameters.yaml` lines that have become dead (always `true`
   or always `false`) now that their guarding feature is gone — the
   #16454-style follow-up.
6. Bumps `MIN_SUPPORTED_PROTOCOL_VERSION` to `new_min` in `version.rs`.
7. Opens one PR for the whole bundle.

Rationale: once `MIN_SUPPORTED_PROTOCOL_VERSION = new_min`, every
`protocol_version >= new_min` holds unconditionally for all callers (either
by `clamp_to_supported_protocol_version`, which clamps up, or by
`assert_supported_protocol_version`, which rejects anything below). So any
`ProtocolFeature` whose `protocol_version <= new_min` has
`feature.enabled(v) == true` for every version the binary ever sees, and
the matching yaml diff collapses into the base `parameters.yaml`.

Do not open PRs between steps and do not pause for user approval on the
grouping — the whole thing is one PR, created in Step 7.

## 1. Read current state and plan

Collect, before touching any file:

- `old_min` = current value of `MIN_SUPPORTED_PROTOCOL_VERSION` in `core/primitives-core/src/version.rs`.
- `new_min` = `old_min + 1`.
- The set of `ProtocolFeature` variants with `protocol_version() <= new_min`
  (read the `impl ProtocolFeature { pub const fn protocol_version(self) ... }` block).
  Call this `deprecated_features`.
- The set of yaml files `{v}.yaml` under
  `core/parameters/res/runtime_configs/` with `v <= new_min`.
  Call this `deprecated_yamls`.
- For each `v > new_min` yaml file, the set of parameter names it mentions
  (needed in Step 3 to classify "overridden later").

Report to the user:
- `old_min → new_min`
- `deprecated_features` (count + names)
- `deprecated_yamls` (filenames)

No approval needed — proceed to Step 2.

## 2. Remove deprecated `ProtocolFeature` variants

For each `F` in `deprecated_features`:

1. **Remove `F` from the `ProtocolFeature` enum** in `core/primitives-core/src/version.rs`.
2. **Remove `F`'s arm** from the `protocol_version()` match. If `F` was in a
   grouped `|` arm, drop just that line; if it was the only variant for
   its version, drop the whole arm.
3. **Inline every call site**:
   - `grep -rn "ProtocolFeature::F\b"` across the workspace.
   - `F.enabled(v)` and `ProtocolFeature::F.enabled(v)` → `true`. Collapse
     the surrounding `if` / `match` / boolean expression: keep the taken
     branch, drop the dead one. `!F.enabled(v)` → `false`, drop the dead
     branch. For `let x = F.enabled(v);` with `x` used downstream, propagate
     the constant and remove the binding when it's no longer referenced.
   - Other uses of `ProtocolFeature::F` as a value (e.g. inserted into a
     set, printed in a log) → remove the use. If removing it leaves an
     empty collection or dead loop, drop that too.
4. **Clean up TODOs** anywhere in the codebase that reference `F` or its
   protocol version as the gating condition for removal (grep for
   `TODO.*F\b`, `TODO.*protocol version.*F`'s version). Resolve them now.
5. **Sweep for dead code and unused data structures** (see "Dead-code
   sweep" below). Collapsing branches in Step 2.3 commonly orphans whole
   structs, enum variants, trait impls, helper fns, or crate-boundary
   types — remove them.

Do not commit or open a PR here; keep going.

## 3. Clean up each deprecated yaml

For each `{v}.yaml` in `deprecated_yamls`, **every** parameter line must
be inlined into `parameters.yaml` — the diffs form a chain validated by
`apply_diff` (each line's `old` must equal the current table value), so
simply dropping a line without updating the base would break the chain
and panic on the next yaml that touches the same parameter.

For each parameter `P: {old: A, new: B}` in `{v}.yaml`:

1. **Update `parameters.yaml`**: set `P`'s base value to `B` (the `new`
   value from `{v}.yaml`). If `P` is not already present, add it in the
   correct section (match the file's grouping conventions). If
   `parameters_testnet.yaml` exists, mirror the change there.
2. **Drop the line** from `{v}.yaml`.
3. **Update `core/parameters/res/runtime_configs/parameters.snap`** to
   reflect `P`'s new base value (match existing alignment).

Then classify `P` as either **overridden later** (its name appears in some
`{w}.yaml` with `w > v`) or **not overridden later**:

- **Overridden later** → stop here for this parameter. Later yamls still
  read it, so it must stay in `enum Parameter` and in all consumer code.
  The `{w}.yaml` entry's `old` value should already match `B` (if it does
  not, something is wrong with the chain — stop and ask).
- **Not overridden later** → `B` is now `P`'s value forever, so dismantle
  the plumbing:
  1. **Remove `P` from the `Parameter` enum** in
     `core/parameters/src/parameter.rs`, and from the
     `impl TryFrom<&ParameterTable> for RuntimeConfig` body in
     `core/parameters/src/parameter_table.rs` (the `params.get(Parameter::P)?`
     line).
  2. **Remove `P` from `ConfigView` / `VMConfigView` entirely** (for
     `wasm_config` / `vm::Config` fields): delete the field from
     `VMConfigView`, from `vm::Config`, and from both `From` impls between
     them. We accept the backward-incompatibility
     ([zulip decision](https://near.zulipchat.com/#narrow/channel/524117-nearone.2Fbetter-engineering/topic/ConfigView.20cleanups/with/631917110)) —
     do not keep the field as `/// Deprecated` with a hardcoded value.
     Also delete the field from `chain/jsonrpc/openapi/openapi.json` and
     `chain/jsonrpc/openapi/openrpc.json`, and bump the API version in both
     (and in `chain/jsonrpc/openapi/src/main.rs`) — see #16454 for the
     diff shape.
  3. **Replace every usage** across the codebase with the hardcoded value
     and collapse dead branches, same as Step 2.3.
  4. Also remove the entry from `parameters.yaml`,
     `parameters_testnet.yaml`, and `parameters.snap` — now that `P` is
     gone from the enum, there's no reader for the base value either.
  5. **Sweep for dead code and unused data structures** (see "Dead-code
     sweep" below) — once the parameter is gone and its guards are
     collapsed, config structs, helper fns and signature arguments often
     become unused.

After this step, each `{v}.yaml` in `deprecated_yamls` is empty but still
on disk (deleted in Step 6).

## 4. Verify intermediate state

Before continuing to the dead-flag follow-up:

- `cargo check -p near-parameters`
- `cargo check --workspace`
- `cargo insta test --accept -p near-parameters` — regenerates snapshots.
  Spot-check the diff:
  - The version-0 snapshot picks up each inlined parameter's new base
    value (`B` from `{v}.yaml`).
  - Snapshots for `w > v` where `w` overrides `P` should stay unchanged
    (they already recorded the post-`{w}` value).
  - Snapshots for `w > v` where `w` does **not** override `P` should also
    stay unchanged — the base moved from `A` to `B` but applying the
    now-missing `{v}.yaml` diff used to move it from `A` to `B` as well.
  - If any snapshot other than version-0 changes value, something is wrong
    — stop and investigate before accepting.

## 5. Inline leftover dead flags from `parameters.yaml`

Beyond the parameters inlined in Step 3, `parameters.yaml` may still carry
flags whose only false→true transition belonged to a now-removed
`ProtocolFeature` or to a yaml that's being deleted. Find them:

- `grep -n "false$\|true$" core/parameters/res/runtime_configs/parameters.yaml`
  for boolean baselines.
- For each such flag `P`:
  - If `P` is read via `params.get(Parameter::P)` and the only call site is
    a check that always holds now (because the gating feature is gone),
    inline `P` to its post-bump value.
  - Remove `P` from `parameters.yaml` and `parameters_testnet.yaml`, from
    `parameters.snap`, from the `Parameter` enum, from
    `parameter_table.rs`, from `vm::Config`, and from `VMConfigView`
    entirely (both `From` impls) — plus the field in `openapi.json` /
    `openrpc.json` and the API version bump. Backward-incompatibility is
    accepted; do not keep a deprecated hardcoded field. (This is a change
    from the older #16454 shape, which did keep the field; follow the
    issue's updated guidance instead.)
  - Collapse all call sites; delete now-unused arguments to helper
    functions (see `transfer_exec_fee` in #16454).
  - **Sweep for dead code and unused data structures** (see "Dead-code
    sweep" below).

This step is often a no-op after Step 3; skip it if nothing qualifies.

## 6. Bump + delete

1. **Bump `MIN_SUPPORTED_PROTOCOL_VERSION`** from `old_min` to `new_min` in
   `core/primitives-core/src/version.rs`.
2. For each `{v}.yaml` in `deprecated_yamls`:
   - Delete `core/parameters/res/runtime_configs/{v}.yaml`.
   - Remove the `(v, include_config!("{v}.yaml"))` entry from
     `CONFIG_DIFFS` in `core/parameters/src/config_store.rs`.
   - Delete `core/parameters/src/snapshots/near_parameters__config_store__tests__{v}.json.snap`.
3. **Verify**:
   - `cargo test -p near-parameters` — in particular
     `config_store::tests::all_configs_are_specified`, which cross-checks
     the yaml file set against `CONFIG_DIFFS`.
   - `cargo check --workspace`.

## 7. Open the single PR

Stage every change from Steps 2–6 and invoke the `pr` skill with
`type: chore, project: runtime, title: bump MIN_SUPPORTED_PROTOCOL_VERSION to <new_min>`.

PR body should:

- Note that `MIN_SUPPORTED_PROTOCOL_VERSION` moves `old_min → new_min`.
- List the removed `ProtocolFeature` variants.
- List the inlined and dropped parameters, and note that any removed
  `VMConfigView` fields are a backward-incompatible RPC change (per the
  [zulip decision](https://near.zulipchat.com/#narrow/channel/524117-nearone.2Fbetter-engineering/topic/ConfigView.20cleanups/with/631917110)).
- List which yaml files and snapshots were deleted.
- Reference `Part of #16564.`

## Dead-code sweep

After every cleanup in Steps 2, 3, and 5 — before you move on — inspect
the surrounding code for things that are now unreachable or orphaned.
Collapsing an `if feature.enabled(...)` guard or deleting a parameter from
a config struct frequently leaves whole items unused, but the Rust
compiler only flags a subset of these (unused `pub` items re-exported
across crates often go unwarned).

Run `cargo check --workspace` and `cargo clippy --workspace` and treat
every new `dead_code` / `unused_imports` / `unused_variables` warning as
in-scope.

Then grep deliberately for the specific shapes the compiler misses:

- **Enum variants** that were only constructed inside the collapsed
  branch. Grep for each variant's name; if the only remaining references
  are the `match` arm plus `Debug`/serde impls, delete the variant.
- **Structs / struct fields** that were only populated or read inside the
  collapsed branch. A field with no live reader can be removed; a struct
  whose only constructor is gone can be deleted along with its impls.
- **Helper functions / methods** whose only call site vanished.
- **Function arguments** that are no longer read inside the body (e.g.
  `transfer_exec_fee` dropping `eth_implicit_accounts_enabled` in #16454)
  — remove them from the signature, not just unused at every call site.
- **Trait impls / `From` conversions** whose target type just became
  unused.
- **Feature flags** (`#[cfg(feature = "...")]`) and `Cargo.toml` entries
  that only guarded the deleted code.
- **Imports and re-exports** that only mentioned the deleted item.
- **Protocol-version constants** (e.g. `pub const SOMETHING_PROTOCOL_VERSION`)
  defined for a now-removed feature.
- **Comments, docstrings, and `TODO`s** that reference the removed
  parameter/feature by name.

If a candidate for removal is `pub` and could plausibly be used by a
downstream crate (near-sdk, near-workspaces, external tooling), surface it
to the user before deleting rather than silently dropping a public item.

## Notes

- Operate on exactly one `MIN_SUPPORTED_PROTOCOL_VERSION` increment per
  invocation. If several bumps are needed, run the skill repeatedly — one
  PR per bump.
- Everything in Steps 2–6 lands in **one PR**. Do not call the `pr` skill
  mid-way or create interim commits for review.
- If classification uncovers anything unexpected (a parameter in `{v}.yaml`
  that isn't in `parameters.yaml` and isn't in `enum Parameter`, or an
  `old` value that disagrees with the current `parameters.yaml` baseline),
  stop and ask the user before touching code.
- If a `ProtocolFeature` arm you want to remove was `cfg`-gated or
  TODO-tagged for a different protocol version than its listed one, pause
  and confirm — the TODO may be the authoritative intent.
- Boolean inlining: `.enabled() → true` means keep the taken branch,
  delete the else; a helper argument that becomes constant should be
  removed from the signature, not just hardcoded at every call site.
